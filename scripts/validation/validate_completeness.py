#!/usr/bin/env python3
"""
validate_completeness.py — check that raw S3 source records appear in the event store.

For each sample: pick a random date, try sources for that date until one has data,
fetch one random MX or SPF row, then verify the event store reflects it.

Usage:
    python validate_completeness.py
    python validate_completeness.py --source zonefiles --n 200
    python validate_completeness.py --source both --n 500 --seed 42
    python validate_completeness.py --n 100 -v
"""

import argparse
import datetime
import hashlib
import os
import random
import sys

import duckdb
import tldextract as _tldextract_lib

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
_env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip())

KEY_ID = os.environ.get('OPENINTEL_KEY_ID', '')
SECRET  = os.environ.get('OPENINTEL_SECRET', '')

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

DATA_DIR   = 'data/toplists'
EVENTS_DIR = os.path.join(_REPO_ROOT, DATA_DIR, 'events')

START_DATE = datetime.date(2016, 1, 22)
END_DATE   = datetime.date(2026, 1, 1)

S3_ENDPOINT = 'storage.dacs.utwente.nl'
S3_BUCKET   = 'openintel'

TOPLIST_SOURCES  = ['umbrella', 'tranco', 'radar', 'alexa', 'majestic']
ZONEFILE_SOURCES = [] # ['ee', 'fr', 'gov', 'li', 'nu', 'se', 'sk']

# (basis, source) → (first_date, last_date or None)
# None end date means still active.
SOURCE_COVERAGE: dict[tuple[str, str], tuple[datetime.date, datetime.date | None]] = {
    ('toplist',  'alexa'):    (datetime.date(2016,  1, 22), datetime.date(2023,  5, 15)),
    ('toplist',  'umbrella'): (datetime.date(2019,  1, 14), None),
    ('toplist',  'tranco'):   (datetime.date(2022,  8, 11), None),
    ('toplist',  'radar'):    (datetime.date(2022, 10,  4), None),
    ('toplist',  'majestic'): (datetime.date(2025,  3, 17), None),
    ('zonefile', 'gov'):      (datetime.date(2017,  5,  1), None),
    ('zonefile', 'ee'):       (datetime.date(2019,  7, 29), None),
    ('zonefile', 'li'):       (datetime.date(2020,  5, 19), None),
    ('zonefile', 'nu'):       (datetime.date(2016,  6,  7), None),
    ('zonefile', 'se'):       (datetime.date(2016,  6,  7), None),
    ('zonefile', 'sk'):       (datetime.date(2022,  5, 11), None),
    ('zonefile', 'fr'):       (datetime.date(2022,  8, 10), None),
}

def _active_sources(sources: list[tuple[str, str]], d: datetime.date) -> list[tuple[str, str]]:
    result = []
    for basis, source in sources:
        cov = SOURCE_COVERAGE.get((basis, source))
        if cov is None:
            result.append((basis, source))  # unknown coverage — include anyway
            continue
        start, end = cov
        if start <= d and (end is None or d <= end):
            result.append((basis, source))
    return result

# ---------------------------------------------------------------------------
# Domain helpers (from preprocess_parquet_v2.py)
# ---------------------------------------------------------------------------
_tld = _tldextract_lib.TLDExtract()

def _reg_domain(domain: str) -> str:
    if not domain:
        return domain
    rd = _tld(domain.lower()).top_domain_under_public_suffix
    return rd if rd else domain.lower()

def _bucket(reg_dom: str) -> int:
    key = (reg_dom or '').lower().encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=4).digest(), 'big') % 256

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _secret_sql() -> str:
    return (
        f"CREATE OR REPLACE SECRET openintel ("
        f"TYPE S3, KEY_ID '{KEY_ID}', SECRET '{SECRET}', "
        f"REGION 'us-east-1', ENDPOINT '{S3_ENDPOINT}', "
        f"URL_STYLE 'path', USE_SSL true);"
    )

def _s3_path(basis: str, source: str, d: datetime.date) -> str:
    if basis == 'toplist':
        return (
            f"s3://{S3_BUCKET}/catalog/warehouse/fdns/data/"
            f"source={source}/date={d}/*.parquet"
        )
    return (
        f"s3://{S3_BUCKET}/category=fdns/type=warehouse/"
        f"source={source}/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/*.parquet"
    )

def _fetch_one(s3_con: duckdb.DuckDBPyConnection,
               basis: str, source: str, d: datetime.date) -> tuple | None:
    """Fetch one random MX or SPF TXT row from S3. Returns (domain, qtype, value) or None."""
    path = _s3_path(basis, source, d)
    try:
        rows = s3_con.execute(f"""
            SELECT query_name, query_type, COALESCE(mx_address, txt_text) AS val
            FROM read_parquet('{path}')
            WHERE (query_type = 'MX'  AND mx_address IS NOT NULL)
               OR (query_type = 'TXT' AND txt_text   IS NOT NULL)
            LIMIT 1
        """).fetchall()
        if rows and rows[0][0]:
            return rows[0]
    except Exception as e:
        msg = str(e)
        if 'No files found' not in msg and "doesn't exist" not in msg:
            print(f"  ! {basis}/{source} {d}: {e}", file=sys.stderr)
    return None

# ---------------------------------------------------------------------------
# Sampling: pick a random date, try sources until one has data
# ---------------------------------------------------------------------------

def draw_samples(
    sources: list[tuple[str, str]],
    n: int,
    rng: random.Random,
) -> list[dict]:
    """
    Collect n samples. For each: pick a random date, shuffle sources, try each
    until one returns a row. Skip the date entirely if no source has data.
    """
    date_range_days = (END_DATE - START_DATE).days

    s3_con = duckdb.connect()
    s3_con.execute("INSTALL aws; LOAD aws;")
    s3_con.execute(_secret_sql())

    samples = []
    attempts = 0
    max_attempts = n * 30  # give up after this many date picks with no data

    while len(samples) < n and attempts < max_attempts:
        attempts += 1
        d = START_DATE + datetime.timedelta(days=rng.randint(0, date_range_days))
        available = _active_sources(sources, d)
        if not available:
            continue  # no source covers this date
        shuffled = rng.sample(available, len(available))

        for basis, source in shuffled:
            row = _fetch_one(s3_con, basis, source, d)
            if row:
                domain, qtype, val = row
                samples.append({
                    'domain':     domain,
                    'query_type': qtype,
                    'date':       d,
                    'raw_value':  val,
                    'source':     f'{basis}/{source}',
                })
                break  # got one for this date, move on

        if len(samples) % 50 == 0 and len(samples) > 0:
            print(f"  {len(samples)}/{n} samples collected ({attempts} date(s) tried) ...")

    s3_con.close()

    if len(samples) < n:
        print(f"  Warning: only collected {len(samples)}/{n} samples after {attempts} attempts.",
              file=sys.stderr)

    return samples

# ---------------------------------------------------------------------------
# Event store lookup
# ---------------------------------------------------------------------------

_NOT_FOUND = object()


def _bucket_files(bucket: int) -> list[str]:
    bdir = os.path.join(EVENTS_DIR, f'bucket={bucket}')
    if not os.path.isdir(bdir):
        return []
    return [
        os.path.join(bdir, f)
        for f in os.listdir(bdir)
        if f.endswith('.parquet')
    ]


def lookup_active_value(con: duckdb.DuckDBPyConnection,
                        domain: str, query_type: str, date: datetime.date):
    """
    Returns:
      _NOT_FOUND   — no event for this domain on or before `date`
      None         — most recent event is a disappearance
      list[str]    — active aggregated value list
    """
    files = _bucket_files(_bucket(_reg_domain(domain)))
    if not files:
        return _NOT_FOUND

    files_sql = ', '.join(f"'{f}'" for f in files)
    try:
        row = con.execute(f"""
            SELECT value
            FROM read_parquet([{files_sql}])
            WHERE domain = '{domain}'
              AND query_type = '{query_type}'
              AND measurement_date <= '{date}'
            ORDER BY measurement_date DESC
            LIMIT 1
        """).fetchone()
    except Exception as e:
        print(f"  ! event store query failed for {domain}: {e}", file=sys.stderr)
        return _NOT_FOUND

    if row is None:
        return _NOT_FOUND
    return row[0]

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(samples: list[dict], verbose: bool) -> dict:
    con = duckdb.connect()
    passed = failed = skipped = 0
    failures = []
    total = len(samples)

    for i, s in enumerate(samples, 1):
        domain    = s['domain']
        qtype     = s['query_type']
        date      = s['date']
        raw_value = s['raw_value']

        active = lookup_active_value(con, domain, qtype, date)

        if active is _NOT_FOUND:
            failed += 1
            failures.append({**s, 'active': None})
            if verbose:
                print(f"  [{i:4d}/{total}] FAIL  (not in event store)  {domain}  {qtype}  {date}")

        elif active is None:
            skipped += 1
            if verbose:
                print(f"  [{i:4d}/{total}] SKIP  (disappeared by {date})  {domain}  {qtype}")

        elif raw_value in active:
            passed += 1
            if verbose:
                print(f"  [{i:4d}/{total}] PASS  {domain}  {qtype}  {date}")

        else:
            failed += 1
            failures.append({**s, 'active': active})
            if verbose:
                print(f"  [{i:4d}/{total}] FAIL  {domain}  {qtype}  {date}")
                print(f"              raw    : {raw_value}")
                print(f"              active : {active}")

    con.close()
    return {'passed': passed, 'failed': failed, 'skipped': skipped,
            'total': total, 'failures': failures}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Completeness validation: S3 raw records → event store."
    )
    parser.add_argument(
        '--source', choices=['toplists', 'zonefiles', 'both'], default='toplists',
    )
    parser.add_argument('--n', type=int, default=500)
    parser.add_argument('--seed', type=int, default=None)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--output', metavar='FILE',
                        help='Write report to this file (default: auto-named in results/validate_completeness/)')
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = args.output or os.path.join(
        _REPO_ROOT, 'results', 'validate_completeness',
        f'completeness_{args.source}_{ts}.txt'
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out_file = open(out_path, 'w')
    import builtins
    _orig_print = builtins.print
    def _tee(*pargs, **kwargs):
        _orig_print(*pargs, **kwargs)
        kw2 = {k: v for k, v in kwargs.items() if k != 'file'}
        _orig_print(*pargs, file=out_file, **kw2)
    builtins.print = _tee

    rng = random.Random(args.seed)

    sources = []
    if args.source in ('toplists', 'both'):
        sources += [('toplist',  s) for s in TOPLIST_SOURCES]
    if args.source in ('zonefiles', 'both'):
        sources += [('zonefile', s) for s in ZONEFILE_SOURCES]

    if not sources:
        print("No sources configured.", file=sys.stderr)
        sys.exit(1)

    if not os.path.isdir(EVENTS_DIR):
        print(f"events dir not found: {EVENTS_DIR}", file=sys.stderr)
        sys.exit(1)

    print(f"Event store : {EVENTS_DIR}")
    print(f"Sources     : {[f'{b}/{s}' for b, s in sources]}")
    print(f"Date range  : {START_DATE} to {END_DATE}")
    print(f"Samples     : {args.n}\n")

    samples = draw_samples(sources, args.n, rng)
    if not samples:
        print("No samples collected.", file=sys.stderr)
        sys.exit(1)

    print(f"\n  Collected {len(samples)} sample(s). Validating against event store ...\n")

    result = validate(samples, args.verbose)

    print(f"\nResults:")
    print(f"  passed  : {result['passed']}")
    print(f"  failed  : {result['failed']}")
    print(f"  skipped : {result['skipped']}  (disappearance active on sample date)")
    print(f"  total   : {result['total']}")

    if result['failures']:
        n_show = min(10, len(result['failures']))
        print(f"\nFirst {n_show} failure(s):")
        for f in result['failures'][:n_show]:
            print(f"  {f['domain']}  {f['query_type']}  {f['date']}  ({f['source']})")
            if f['active'] is None:
                print(f"    → not found in event store")
            else:
                print(f"    → raw value    : {f['raw_value']}")
                print(f"    → active value : {f['active']}")
        if len(result['failures']) > n_show:
            print(f"  ... and {len(result['failures']) - n_show} more")

    builtins.print = _orig_print
    out_file.close()
    print(f"Report written to {out_path}")

    if result['failed'] > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
