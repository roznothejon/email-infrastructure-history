#!/usr/bin/env python3
"""
validate_performance.py — event store vs raw S3 query latency.

For each test domain, times:
  1. Event store exact   (domain = X)       — one bucket, full history
  2. Event store suffix  (domain LIKE %.X)  — subdomains too, same bucket
  3. S3 exact            (one source, N days via hive partitioning)
  4. S3 suffix           (same, LIKE on query_name — no pushdown)

Usage:
    python validate_performance.py
    python validate_performance.py --s3-source umbrella --s3-days 365
    python validate_performance.py --domains google.com utwente.nl
    python validate_performance.py --dataset toplists --s3-days 90
"""

import argparse
import datetime
import hashlib
import os
import sys
import time

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
SECRET = os.environ.get('OPENINTEL_SECRET', '')

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

S3_ENDPOINT = 'storage.dacs.utwente.nl'
S3_BUCKET   = 'openintel'

# Full dataset date range
DATASET_START = datetime.date(2016, 1, 22)
DATASET_END   = datetime.date(2026, 1, 1)
DATASET_DAYS  = (DATASET_END - DATASET_START).days  # ~3630

DEFAULT_DOMAINS = [
    'google.com',
    'microsoft.com',
    'amazon.com',
    'mailchimp.com',
    'sendgrid.com',
    'utwente.nl',
    'tudelft.nl',
    'protonmail.com',
]

DATASET_CONFIG = {
    'toplists': {
        'data_dir':       'data/toplists',
        'default_source': 'umbrella',
        's3_path_tpl':    'catalog/warehouse/fdns/data/source={source}/date=*/*.parquet',
        'all_sources':    ['umbrella', 'tranco', 'radar', 'alexa', 'majestic'],
    },
    'zonefiles': {
        'data_dir':       'data/zonefiles',
        'default_source': 'se',
        's3_path_tpl':    'category=fdns/type=warehouse/source={source}/year=*/month=*/day=*/*.parquet',
        'all_sources':    ['ee', 'fr', 'gov', 'li', 'nu', 'se', 'sk'],
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_tld = _tldextract_lib.TLDExtract()

def _fqdn(domain: str) -> str:
    """Ensure trailing dot for FQDN queries."""
    d = domain.lower().rstrip('.')
    return d + '.'

def _reg_domain(domain: str) -> str:
    rd = _tld(domain.lower()).top_domain_under_public_suffix
    return rd if rd else domain.lower().rstrip('.')

def _bucket(reg_dom: str) -> int:
    key = reg_dom.lower().encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=4).digest(), 'big') % 256

def _bucket_files(events_dir: str, bucket: int) -> list[str]:
    bdir = os.path.join(events_dir, f'bucket={bucket}')
    if not os.path.isdir(bdir):
        return []
    return [os.path.join(bdir, f) for f in os.listdir(bdir) if f.endswith('.parquet')]

def _secret_sql() -> str:
    return (
        f"CREATE OR REPLACE SECRET openintel ("
        f"TYPE S3, KEY_ID '{KEY_ID}', SECRET '{SECRET}', "
        f"REGION 'us-east-1', ENDPOINT '{S3_ENDPOINT}', "
        f"URL_STYLE 'path', USE_SSL true);"
    )

def _run(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[float, int]:
    """Execute SQL, return (elapsed_s, row_count)."""
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    return time.perf_counter() - t0, len(rows)

def _run_streaming(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[float, int]:
    """Like _run but fetches in chunks — avoids materialising all rows in Python."""
    t0 = time.perf_counter()
    cur = con.execute(sql)
    count = 0
    while True:
        chunk = cur.fetchmany(1000)
        if not chunk:
            break
        count += len(chunk)
    return time.perf_counter() - t0, count

# ---------------------------------------------------------------------------
# Event store queries
# ---------------------------------------------------------------------------

def event_exact(con: duckdb.DuckDBPyConnection,
                events_dir: str, domain: str) -> tuple[float, int]:
    fq = _fqdn(domain)
    files = _bucket_files(events_dir, _bucket(_reg_domain(domain)))
    if not files:
        return 0.0, 0
    files_sql = ', '.join(f"'{f}'" for f in files)
    t, rows = _run(con, f"""
        SELECT domain, query_type, measurement_date, value
        FROM read_parquet([{files_sql}])
        WHERE domain = '{fq}'
        ORDER BY measurement_date
    """)
    # Warm run then real run to exclude DuckDB startup overhead.
    t, rows = _run(con, f"""
        SELECT domain, query_type, measurement_date, value
        FROM read_parquet([{files_sql}])
        WHERE domain = '{fq}'
        ORDER BY measurement_date
    """)
    return t, rows

def event_suffix(con: duckdb.DuckDBPyConnection,
                 events_dir: str, domain: str) -> tuple[float, int]:
    fq = _fqdn(domain)
    files = _bucket_files(events_dir, _bucket(_reg_domain(domain)))
    if not files:
        return 0.0, 0
    files_sql = ', '.join(f"'{f}'" for f in files)
    sql = f"""
        SELECT domain, query_type, measurement_date, value
        FROM read_parquet([{files_sql}])
        WHERE domain = '{fq}' OR domain LIKE '%.{fq}'
        ORDER BY measurement_date
    """
    _run(con, sql)  # warmup
    return _run(con, sql)

# ---------------------------------------------------------------------------
# S3 queries
# ---------------------------------------------------------------------------

def s3_exact(con: duckdb.DuckDBPyConnection,
             s3_path: str, domain: str, start: datetime.date) -> tuple[float, int]:
    fq = _fqdn(domain)
    return _run(con, f"""
        SELECT query_name, query_type, "date", COALESCE(mx_address, txt_text) AS val
        FROM read_parquet('s3://{S3_BUCKET}/{s3_path}', hive_partitioning=true)
        WHERE "date" >= '{start}'
          AND query_name = '{fq}'
          AND ((query_type = 'MX'  AND mx_address IS NOT NULL)
            OR (query_type = 'TXT' AND txt_text   IS NOT NULL))
        ORDER BY "date"
    """)

def s3_suffix(con: duckdb.DuckDBPyConnection,
              s3_path: str, domain: str, start: datetime.date) -> tuple[float, int]:
    fq = _fqdn(domain)
    return _run(con, f"""
        SELECT query_name, query_type, "date", COALESCE(mx_address, txt_text) AS val
        FROM read_parquet('s3://{S3_BUCKET}/{s3_path}', hive_partitioning=true)
        WHERE "date" >= '{start}'
          AND (query_name = '{fq}' OR query_name LIKE '%.{fq}')
          AND ((query_type = 'MX'  AND mx_address IS NOT NULL)
            OR (query_type = 'TXT' AND txt_text   IS NOT NULL))
        ORDER BY "date"
    """)

def _s3_full_scan(con: duckdb.DuckDBPyConnection,
                  path_tpl: str, sources: list[str],
                  where_domain: str) -> tuple[float, int]:
    """Scan all dates per source, one source at a time. 4GB memory cap forces spill."""
    total_t   = 0.0
    total_rows = 0
    for source in sources:
        path = path_tpl.format(source=source)
        try:
            t, rows = _run_streaming(con, f"""
                SELECT query_name, query_type, COALESCE(mx_address, txt_text) AS val
                FROM read_parquet('s3://{S3_BUCKET}/{path}', hive_partitioning=true)
                WHERE {where_domain}
                  AND ((query_type = 'MX'  AND mx_address IS NOT NULL)
                    OR (query_type = 'TXT' AND txt_text   IS NOT NULL))
            """)
            total_t    += t
            total_rows += rows
            print(f"\n    [{source}] {t:.1f}s, {rows} rows", end='', flush=True)
        except Exception as e:
            print(f"\n    [{source}] skipped: {e}", end='', flush=True)
    return total_t, total_rows

def s3_full_exact(con: duckdb.DuckDBPyConnection,
                  path_tpl: str, sources: list[str], domain: str) -> tuple[float, int]:
    fq = _fqdn(domain)
    return _s3_full_scan(con, path_tpl, sources, f"query_name = '{fq}'")

def s3_full_suffix(con: duckdb.DuckDBPyConnection,
                   path_tpl: str, sources: list[str], domain: str) -> tuple[float, int]:
    fq = _fqdn(domain)
    return _s3_full_scan(con, path_tpl, sources,
                         f"(query_name = '{fq}' OR query_name LIKE '%.{fq}')")

# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _speedup(event_t: float, s3_t: float) -> str:
    if event_t <= 0:
        return '    —'
    return f'{s3_t / event_t:6.0f}x'

def print_result(domain: str, bucket: int, n_files: int,
                 e_exact: tuple, e_suffix: tuple,
                 s3_ex: tuple, s3_sf: tuple,
                 s3_days: int, active_sources: int,
                 s3_full_ex: tuple | None = None,
                 s3_full_sf: tuple | None = None) -> None:
    ee_t, ee_r = e_exact
    es_t, es_r = e_suffix
    sx_t, sx_r = s3_ex
    ss_t, ss_r = s3_sf

    speedup_exact  = _speedup(ee_t, sx_t)
    speedup_suffix = _speedup(es_t, ss_t)

    # Extrapolate to full S3 scan: all days × all active sources
    extra_factor = (DATASET_DAYS * active_sources) / max(s3_days, 1)
    extrap_exact  = sx_t * extra_factor
    extrap_suffix = ss_t * extra_factor

    print(f"\n  {domain}  (bucket={bucket}, {n_files} file(s))")
    print(f"  {'─'*66}")
    print(f"  {'Query':<28} {'Time':>8}  {'Rows':>6}  {'Scope'}")
    print(f"  {'─'*66}")
    print(f"  {'Event exact':<28} {ee_t:>7.3f}s  {ee_r:>6}  full history, all sources")
    print(f"  {'Event suffix (subdomains)':<28} {es_t:>7.3f}s  {es_r:>6}  full history, all sources")
    print(f"  {'S3 exact':<28} {sx_t:>7.2f}s  {sx_r:>6}  {s3_days}d window, 1 source")
    print(f"  {'S3 suffix':<28} {ss_t:>7.2f}s  {ss_r:>6}  {s3_days}d window, 1 source")
    if s3_full_ex is not None:
        fx_t, fx_r = s3_full_ex
        ff_t, ff_r = s3_full_sf
        print(f"  {'S3 full exact (all srcs/dates)':<28} {fx_t:>7.2f}s  {fx_r:>6}  full history, all sources")
        print(f"  {'S3 full suffix':<28} {ff_t:>7.2f}s  {ff_r:>6}  full history, all sources")
        print(f"  {'─'*66}")
        print(f"  Speedup exact  (windowed):  {speedup_exact}")
        print(f"  Speedup exact  (full S3):   {_speedup(ee_t, fx_t)}")
        print(f"  Speedup suffix (windowed):  {speedup_suffix}")
        print(f"  Speedup suffix (full S3):   {_speedup(es_t, ff_t)}")
    else:
        print(f"  {'─'*66}")
        print(f"  Speedup (exact):            {speedup_exact}  "
              f"[full S3 est. {extrap_exact:>6.0f}s → ~{s3_ex[0] and extrap_exact/ee_t or 0:.0f}x]")
        print(f"  Speedup (suffix):           {speedup_suffix}  "
              f"[full S3 est. {extrap_suffix:>6.0f}s → ~{s3_sf[0] and extrap_suffix/es_t or 0:.0f}x]")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Compare query performance: event store vs raw S3.'
    )
    parser.add_argument('--domains', nargs='+', default=DEFAULT_DOMAINS,
                        metavar='DOMAIN')
    parser.add_argument('--dataset', choices=['toplists', 'zonefiles'],
                        default='toplists')
    parser.add_argument('--s3-source', default=None,
                        help='S3 source to use as baseline (default: per dataset)')
    parser.add_argument('--s3-days', type=int, default=365,
                        help='Days of S3 history to scan as baseline (default: 365)')
    parser.add_argument('--full-s3', action='store_true',
                        help='Also run true naive S3 scan: all sources, all dates, no filters (slow)')
    parser.add_argument('--out', default=None, metavar='FILE',
                        help='Save output to file (default: auto-named in results/)')
    args = parser.parse_args()

    cfg        = DATASET_CONFIG[args.dataset]
    events_dir = os.path.join(_REPO_ROOT, cfg['data_dir'], 'events')
    s3_source  = args.s3_source or cfg['default_source']
    s3_path    = cfg['s3_path_tpl'].format(source=s3_source)
    s3_start   = DATASET_END - datetime.timedelta(days=args.s3_days)

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = args.out or os.path.join(
        _REPO_ROOT, 'results', 'validate_performance',
        f'performance_{args.dataset}_{ts}.txt'
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Tee: write to both stdout and the output file.
    out_file = open(out_path, 'w')
    import builtins
    _orig_print = builtins.print
    def _tee(*pargs, **kwargs):
        _orig_print(*pargs, **kwargs)
        kw2 = {k: v for k, v in kwargs.items() if k != 'file'}
        _orig_print(*pargs, file=out_file, **kw2)
    builtins.print = _tee

    print(f"Dataset     : {args.dataset}")
    print(f"Events dir  : {events_dir}")
    print(f"S3 baseline : {s3_source}, {args.s3_days} days ({s3_start} → {DATASET_END})")
    if args.full_s3:
        print(f"S3 full     : all sources, all dates (this will be slow)")
    print(f"Domains     : {args.domains}")

    if not os.path.isdir(events_dir):
        print(f"\nEvents dir not found: {events_dir}", file=sys.stderr)
        sys.exit(1)

    local_con = duckdb.connect()
    local_con.execute("SET memory_limit='12GB'")

    s3_con = duckdb.connect()
    s3_con.execute("SET memory_limit='12GB'")
    s3_con.execute("SET max_temp_directory_size='50GB'")  # spill to disk freely
    s3_con.execute("SET threads=6")
    s3_con.execute("INSTALL aws; LOAD aws;")
    s3_con.execute(_secret_sql())

    print(f"\n{'═'*70}")
    print(f"  {'Domain':<22}  Event exact  Event suffix  S3 exact  S3 suffix  Speedup")
    print(f"{'═'*70}")

    summary_rows = []

    for domain in args.domains:
        bucket  = _bucket(_reg_domain(domain))
        n_files = len(_bucket_files(events_dir, bucket))

        print(f"\n  {domain} ...", end='', flush=True)

        ee = event_exact(local_con, events_dir, domain)
        es = event_suffix(local_con, events_dir, domain)
        print(f" event done,", end='', flush=True)

        sx = s3_exact(s3_con, s3_path, domain, s3_start)
        ss = s3_suffix(s3_con, s3_path, domain, s3_start)
        print(f" S3 done.", end='', flush=True)

        fx = ff = None
        if args.full_s3:
            print(f"\n  -- full S3 exact --", flush=True)
            fx = s3_full_exact(s3_con, cfg['s3_path_tpl'], cfg['all_sources'], domain)
            print(f"\n  -- full S3 suffix --", flush=True)
            ff = s3_full_suffix(s3_con, cfg['s3_path_tpl'], cfg['all_sources'], domain)
            print(f"\n  -- full S3 done --", end='')
        print()

        print_result(domain, bucket, n_files, ee, es, sx, ss,
                     args.s3_days, len(cfg['all_sources']), fx, ff)

        summary_rows.append((domain, ee, es, sx, ss, fx, ff))

    # Summary table
    print(f"\n\n{'═'*80}")
    header = f"  Summary — event exact vs S3 ({args.s3_days}d window)"
    if args.full_s3:
        header += " + full S3"
    print(header)
    print(f"{'═'*80}")
    cols = f"  {'Domain':<22}  {'E-exact':>8}  {'S3-exact':>9}  {'Speedup':>8}"
    if args.full_s3:
        cols += f"  {'S3-full':>9}  {'Speedup-full':>12}"
    print(cols)
    print(f"  {'─'*76}")
    for row in summary_rows:
        domain, ee, es, sx, ss, fx, ff = row
        sp  = f"{sx[0]/ee[0]:.0f}x" if ee[0] > 0 and sx[0] > 0 else "—"
        line = f"  {domain:<22}  {ee[0]:>7.3f}s  {sx[0]:>8.2f}s  {sp:>8}"
        if args.full_s3 and fx is not None:
            sp2 = f"{fx[0]/ee[0]:.0f}x" if ee[0] > 0 and fx[0] > 0 else "—"
            line += f"  {fx[0]:>8.2f}s  {sp2:>12}"
        print(line)

    local_con.close()
    s3_con.close()
    builtins.print = _orig_print
    out_file.close()
    _orig_print(f"\nOutput saved to: {out_path}")

if __name__ == '__main__':
    main()
