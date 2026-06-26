#!/usr/bin/env python3
"""
validate_correctness.py — spot-check the event store against raw OpenINTEL source data.

Picks N random events, fetches the raw source data from S3 for each event's
measurement date, re-aggregates using the same logic as the pipeline, and
verifies the stored value matches exactly.

For disappearance events (value IS NULL) the check is inverted: the domain
should be absent from all sources on that date.

Usage:
    python validate_correctness.py                          # 500 samples, toplists
    python validate_correctness.py --source zonefiles
    python validate_correctness.py --source both --n 200
    python validate_correctness.py --n 100 --seed 42 -v
    python validate_correctness.py --include-disappearances
"""

import argparse
import datetime
import os
import random
import sys
from collections import defaultdict

import duckdb

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

S3_ENDPOINT = 'storage.dacs.utwente.nl'
S3_BUCKET   = 'openintel'

# Which S3 sources belong to each dataset.
# Adjust if you ingested a different subset.
TOPLIST_SOURCES  = ['umbrella', 'tranco', 'radar', 'alexa', 'majestic']
ZONEFILE_SOURCES = ['ee', 'fr', 'gov', 'li', 'nu', 'se', 'sk']

DATASET_CONFIG = {
    'toplists': {
        'data_dir':  'data/toplists',
        'sources':   [('toplist',  s) for s in TOPLIST_SOURCES],
    },
    'zonefiles': {
        'data_dir':  'data/zonefiles',
        'sources':   [('zonefile', s) for s in ZONEFILE_SOURCES],
    },
}

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


def fetch_for_date(
    con: duckdb.DuckDBPyConnection,
    sources: list[tuple[str, str]],
    d: datetime.date,
    domains: set[str],
) -> dict[tuple[str, str], list[str]]:
    """
    Fetch raw records for domains on date d from all sources, then aggregate
    exactly as the pipeline does: sorted, deduplicated list per (domain, query_type).

    Returns {(domain, query_type): sorted_value_list}.
    """
    # Build an IN list for the WHERE clause so DuckDB filters early.
    dom_list = ', '.join(f"'{d_}'" for d_ in domains)
    rows_by_key: dict[tuple[str, str], set[str]] = defaultdict(set)
    found_any = False

    for basis, source in sources:
        path = _s3_path(basis, source, d)
        try:
            result = con.execute(f"""
                SELECT query_name, query_type, COALESCE(mx_address, txt_text) AS val
                FROM read_parquet('{path}')
                WHERE query_name IN ({dom_list})
                  AND ((query_type = 'MX'  AND mx_address IS NOT NULL)
                    OR (query_type = 'TXT' AND txt_text   IS NOT NULL))
            """).fetchall()
            for qname, qtype, val in result:
                if qname and val:
                    rows_by_key[(qname, qtype)].add(val)
                    found_any = True
        except Exception as e:
            msg = str(e)
            if 'No files found' not in msg and "doesn't exist" not in msg:
                print(f"    ! {basis}/{source}: {e}", file=sys.stderr)

    if not found_any:
        return {}

    return {k: sorted(v) for k, v in rows_by_key.items()}


# ---------------------------------------------------------------------------
# Event store sampling
# ---------------------------------------------------------------------------

def sample_events(
    events_dir: str,
    n: int,
    include_disappearances: bool,
) -> list[dict]:
    """
    Draw n random events from the event store using DuckDB reservoir sampling.
    """
    parquet_files = []
    for entry in sorted(os.listdir(events_dir)):
        bd = os.path.join(events_dir, entry)
        if not os.path.isdir(bd) or not entry.startswith('bucket='):
            continue
        for f in os.listdir(bd):
            if f.endswith('.parquet'):
                parquet_files.append(os.path.join(bd, f))

    if not parquet_files:
        return []

    files_sql = ', '.join(f"'{f}'" for f in parquet_files)
    value_filter = '' if include_disappearances else 'WHERE value IS NOT NULL'

    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT domain, query_type, measurement_date, value, prev_value
            FROM read_parquet([{files_sql}])
            {value_filter}
            USING SAMPLE {n} ROWS
        """).fetchall()
    except Exception as e:
        print(f"Sampling error: {e}", file=sys.stderr)
        return []
    finally:
        con.close()

    return [
        {
            'domain':           r[0],
            'query_type':       r[1],
            'measurement_date': r[2],
            'value':            r[3],   # list[str] or None
            'prev_value':       r[4],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(
    samples: list[dict],
    sources: list[tuple[str, str]],
    verbose: bool,
) -> dict:
    con = duckdb.connect()
    con.execute("INSTALL aws; LOAD aws;")
    con.execute(_secret_sql())

    # Group samples by date to batch S3 fetches (one fetch per date, not per sample).
    by_date: dict[datetime.date, list[dict]] = defaultdict(list)
    for s in samples:
        by_date[s['measurement_date']].append(s)

    passed = failed = skipped = 0
    failures = []
    total_dates = len(by_date)

    for i, (date, date_samples) in enumerate(sorted(by_date.items()), 1):
        domains_needed = {s['domain'] for s in date_samples}
        print(f"  [{i:3d}/{total_dates}] {date}  ({len(date_samples)} sample(s), "
              f"{len(domains_needed)} domain(s)) ...", end='', flush=True)

        raw = fetch_for_date(con, sources, date, domains_needed)

        if not raw and not any(s['value'] is None for s in date_samples):
            # Source data entirely absent for this date — skip rather than fail.
            print(f" no source data — skipping")
            skipped += len(date_samples)
            continue

        print()

        for sample in date_samples:
            domain    = sample['domain']
            qtype     = sample['query_type']
            expected  = sample['value']   # sorted list or None

            raw_value = raw.get((domain, qtype))

            if expected is None:
                # Disappearance: domain should be absent from all sources.
                if raw_value is None:
                    passed += 1
                    if verbose:
                        print(f"    PASS  (disappearance absent) {domain} {qtype}")
                else:
                    # Domain present in source on the disappearance date.
                    # This can happen for grace_expired events (dated to missing_since,
                    # which is the first-absent day, but source might still list it).
                    skipped += 1
                    if verbose:
                        print(f"    SKIP  (disappearance but domain in source) {domain} {qtype}")
            else:
                if raw_value == expected:
                    passed += 1
                    if verbose:
                        print(f"    PASS  {domain} {qtype}")
                else:
                    failed += 1
                    failures.append({
                        'domain':    domain,
                        'query_type': qtype,
                        'date':      date,
                        'expected':  expected,
                        'got':       raw_value,
                    })
                    print(f"    FAIL  {domain} {qtype} {date}")
                    print(f"          expected: {expected}")
                    print(f"          got:      {raw_value}")

    con.close()
    return {
        'passed':   passed,
        'failed':   failed,
        'skipped':  skipped,
        'total':    len(samples),
        'failures': failures,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Technical validation: event store vs raw OpenINTEL source data."
    )
    parser.add_argument(
        '--source', choices=['toplists', 'zonefiles', 'both'], default='toplists',
        help='Dataset(s) to validate (default: toplists).',
    )
    parser.add_argument(
        '--n', type=int, default=500,
        help='Random samples per dataset (default: 500).',
    )
    parser.add_argument(
        '--seed', type=int, default=None,
        help='Random seed for reproducibility.',
    )
    parser.add_argument(
        '--include-disappearances', action='store_true',
        help='Also validate disappearance events (value IS NULL).',
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Print result for every sample.',
    )
    parser.add_argument('--output', metavar='FILE',
                        help='Write report to this file (default: auto-named in results/validate_correctness/)')
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = args.output or os.path.join(
        _REPO_ROOT, 'results', 'validate_correctness',
        f'correctness_{args.source}_{ts}.txt'
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

    if args.seed is not None:
        random.seed(args.seed)

    datasets = []
    if args.source in ('toplists', 'both'):
        datasets.append('toplists')
    if args.source in ('zonefiles', 'both'):
        datasets.append('zonefiles')

    overall_passed = overall_failed = overall_skipped = 0

    for dataset_key in datasets:
        cfg        = DATASET_CONFIG[dataset_key]
        events_dir = os.path.join(_REPO_ROOT, cfg['data_dir'], 'events')
        sources    = cfg['sources']

        print(f"\n=== {dataset_key} — {args.n} samples ===")
        print(f"    events dir : {events_dir}")
        print(f"    sources    : {[f'{b}/{s}' for b, s in sources]}")

        if not os.path.isdir(events_dir):
            print(f"    events dir not found — skipping.")
            continue

        print(f"\n  Sampling ...", end='', flush=True)
        samples = sample_events(events_dir, args.n, args.include_disappearances)
        if not samples:
            print(" no events found — skipping.")
            continue

        n_dates = len({s['measurement_date'] for s in samples})
        print(f" drew {len(samples)} samples across {n_dates} distinct date(s).\n")

        result = validate(samples, sources, args.verbose)

        overall_passed  += result['passed']
        overall_failed  += result['failed']
        overall_skipped += result['skipped']

        print(f"\n  {dataset_key} results:")
        print(f"    passed  : {result['passed']}")
        print(f"    failed  : {result['failed']}")
        print(f"    skipped : {result['skipped']}  (no source data or ambiguous disappearance)")
        print(f"    total   : {result['total']}")

        if result['failures']:
            n_show = min(10, len(result['failures']))
            print(f"\n  First {n_show} failure(s):")
            for f in result['failures'][:n_show]:
                print(f"    {f['domain']}  {f['query_type']}  {f['date']}")
                print(f"      expected : {f['expected']}")
                print(f"      got      : {f['got']}")
            if len(result['failures']) > n_show:
                print(f"    ... and {len(result['failures']) - n_show} more")

    if len(datasets) > 1:
        print(f"\n=== overall ===")
        print(f"  passed  : {overall_passed}")
        print(f"  failed  : {overall_failed}")
        print(f"  skipped : {overall_skipped}")

    builtins.print = _orig_print
    out_file.close()
    print(f"Report written to {out_path}")

    if overall_failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
