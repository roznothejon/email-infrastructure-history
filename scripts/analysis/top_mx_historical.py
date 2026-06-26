#!/usr/bin/env python3
"""
Report historically most common MX servers across monthly S3 snapshots (2016-present).

Queries OpenINTEL S3 directly, sampling the 15th of each month from January 2016
to today. Captures records not present in current state.parquet (retired providers,
changed domains, etc.).

Usage:
  python scripts/analysis/top_mx_historical.py [--source {toplists,zonefiles,both}]
                                                [--top N] [--min-count M]

Expected runtime: 25-50 min for --source both (1,500 S3 reads).
"""

import argparse
import builtins
import datetime
import os
import sys
import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import tldextract

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------------------
# S3 configuration  (mirrors preprocess_parquet_v2.py)
# ---------------------------------------------------------------------------
import os as _os
_env_path = _REPO_ROOT / '.env'
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                _os.environ.setdefault(_k.strip(), _v.strip())

KEY_ID      = _os.environ.get('OPENINTEL_KEY_ID', '')
SECRET      = _os.environ.get('OPENINTEL_SECRET', '')
S3_ENDPOINT = 'storage.dacs.utwente.nl'
# Toplists live in the private bucket; zonefiles in the public bucket
S3_BUCKET_PRIVATE = 'openintel'
S3_BUCKET_PUBLIC  = 'openintel-public'

TOPLIST_SOURCES  = ["umbrella", "tranco", "radar", "alexa", "majestic"]
ZONEFILE_SOURCES = ["ee", "fr", "gov", "li", "nu", "se", "sk"]

_MISSING_ERRS = ("No files found", "does not exist", "404", "HTTP Error",
                 "NoSuchKey", "Object not found")


# ---------------------------------------------------------------------------
# S3 helpers  (verbatim from preprocess_parquet_v2.py)
# ---------------------------------------------------------------------------

def _setup_secrets(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL aws; LOAD aws;")
    con.execute(
        f"CREATE OR REPLACE SECRET openintel_private ("
        f"TYPE S3, KEY_ID '{KEY_ID}', SECRET '{SECRET}', "
        f"REGION 'us-east-1', ENDPOINT '{S3_ENDPOINT}', "
        f"URL_STYLE 'path', USE_SSL true, "
        f"SCOPE 's3://{S3_BUCKET_PRIVATE}/');"
    )
    con.execute(
        f"CREATE OR REPLACE SECRET openintel_public ("
        f"TYPE S3, KEY_ID '', SECRET '', "
        f"REGION 'us-east-1', ENDPOINT '{S3_ENDPOINT}', "
        f"URL_STYLE 'path', USE_SSL true, "
        f"SCOPE 's3://{S3_BUCKET_PUBLIC}/');"
    )


def _s3_path(basis: str, source: str, d: datetime.date) -> str:
    if basis == "toplist":
        return (
            f"s3://{S3_BUCKET_PRIVATE}/catalog/warehouse/fdns/data/"
            f"source={source}/date={d}/*.parquet"
        )
    return (
        f"s3://{S3_BUCKET_PUBLIC}/fdns/basis=zonefile/"
        f"source={source}/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/*.parquet"
    )


# ---------------------------------------------------------------------------
# Output helpers  (verbatim from top_records.py)
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print("=" * 72)


def print_table(rows: list, headers: list[str]) -> None:
    if not rows:
        print("  (no results)")
        return
    str_rows = [[str(v) for v in row] for row in rows]
    widths = [max(len(h), max(len(r[i]) for r in str_rows)) for i, h in enumerate(headers)]
    header_line = "  " + "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "  " + "  ".join("-" * w for w in widths)
    print(header_line)
    print(sep)
    for row in str_rows:
        parts = []
        for i, v in enumerate(row):
            is_numeric = i > 0 and v.replace(",", "").replace(".", "").replace("%", "").lstrip("-").isdigit()
            parts.append(v.rjust(widths[i]) if is_numeric else v.ljust(widths[i]))
        print("  " + "  ".join(parts))


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def sample_dates() -> list[datetime.date]:
    dates = []
    today = datetime.date.today()
    year, month = 2016, 1
    while True:
        d = datetime.date(year, month, 15)
        if d > today:
            break
        dates.append(d)
        month += 1
        if month > 12:
            month = 1
            year += 1
    return dates


def sources_for(source_arg: str) -> list[tuple[str, str]]:
    pairs = []
    if source_arg in ("toplists", "both"):
        pairs += [("toplist", s) for s in TOPLIST_SOURCES]
    if source_arg in ("zonefiles", "both"):
        pairs += [("zonefile", s) for s in ZONEFILE_SOURCES]
    return pairs


# ---------------------------------------------------------------------------
# Accumulation
# ---------------------------------------------------------------------------

def setup_connection() -> tuple[duckdb.DuckDBPyConnection, str]:
    work_dir = _REPO_ROOT / 'tmp'
    work_dir.mkdir(exist_ok=True)
    tmp = str(work_dir / f'mx_hist_{os.getpid()}.duckdb')
    con = duckdb.connect(tmp)
    con.execute("SET memory_limit='14GB'")
    con.execute(f"SET temp_directory='{work_dir}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")
    _setup_secrets(con)
    con.execute("""
        CREATE TABLE _mx_raw (
            domain  VARCHAR,
            mx_val  VARCHAR,
            PRIMARY KEY (domain, mx_val)
        )
    """)
    return con, tmp


def try_ingest(con: duckdb.DuckDBPyConnection, basis: str, source: str,
               d: datetime.date) -> tuple[bool, int]:
    path = _s3_path(basis, source, d)
    try:
        before = con.execute("SELECT COUNT(*) FROM _mx_raw").fetchone()[0]
        con.execute(f"""
            INSERT OR IGNORE INTO _mx_raw
            SELECT DISTINCT lower(query_name), mx_address
            FROM read_parquet('{path}')
            WHERE query_type = 'MX'
              AND mx_address IS NOT NULL
              AND mx_address != ''
        """)
        after = con.execute("SELECT COUNT(*) FROM _mx_raw").fetchone()[0]
        return True, after - before
    except Exception as e:
        err = str(e)
        if any(s in err for s in _MISSING_ERRS):
            return False, 0
        print(f"  ! {basis}/{source} {d}: {e}", file=sys.stderr)
        return False, 0


def accumulate(con: duckdb.DuckDBPyConnection, dates: list[datetime.date],
               src_pairs: list[tuple[str, str]]) -> tuple[int, int]:
    loaded = skipped = 0
    for d in dates:
        print(f"\n[{d}]", flush=True)
        for basis, source in src_pairs:
            t0 = time.time()
            ok, rows = try_ingest(con, basis, source, d)
            elapsed = time.time() - t0
            label = f"{basis[:4]}/{source}"
            if ok:
                print(f"  {label:<20}  {rows:>10,} rows  ({elapsed:.1f}s)", flush=True)
                loaded += 1
            else:
                print(f"  {label:<20}  no data", flush=True)
                skipped += 1
    return loaded, skipped


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def count_mx_domains(con: duckdb.DuckDBPyConnection) -> int:
    # Two-step: DISTINCT domains first, then COUNT — avoids non-spillable COUNT(DISTINCT)
    return con.execute("""
        SELECT COUNT(*) FROM (SELECT DISTINCT domain FROM _mx_raw WHERE mx_val IS NOT NULL)
    """).fetchone()[0]


def mx_analysis(con: duckdb.DuckDBPyConnection, top_n: int, min_count: int,
                total: int) -> None:
    limit = f"LIMIT {top_n}" if top_n > 0 else ""
    rows = con.execute(f"""
        WITH pairs AS (
            SELECT DISTINCT
                lower(rtrim(regexp_replace(mx_val, '^\\d+\\s+', ''), '.')) AS mx_server,
                domain
            FROM _mx_raw
            WHERE mx_val IS NOT NULL AND mx_val != ''
        ),
        ranked AS (
            SELECT mx_server, COUNT(*) AS domains
            FROM pairs
            GROUP BY mx_server
            HAVING domains >= {min_count} AND mx_server != ''
            ORDER BY domains DESC
            {limit}
        )
        SELECT
            row_number() OVER () AS rank,
            mx_server,
            printf('%,d', domains) AS domains,
            printf('%.2f%%', domains * 100.0 / {total}) AS pct
        FROM ranked
    """).fetchall()
    print_table(rows, ["rank", "mx_server", "domains", "pct"])
    print(f"\n  {len(rows)} entries shown  |  total distinct domains with MX: {total:,}")


def mx_provider_analysis(con: duckdb.DuckDBPyConnection, top_n: int, min_count: int,
                         total: int) -> None:
    print("  Building MX provider map via tldextract ...", end="", flush=True)
    unique_mx = con.execute("""
        SELECT DISTINCT lower(rtrim(regexp_replace(mx_val, '^\\d+\\s+', ''), '.')) AS mx_server
        FROM _mx_raw
        WHERE mx_val IS NOT NULL AND mx_val != ''
    """).fetchall()

    mapping: dict[str, str] = {}
    for (mx,) in unique_mx:
        if mx:
            ext = tldextract.extract(mx)
            mapping[mx] = (
                f"{ext.domain}.{ext.suffix}" if ext.domain and ext.suffix else mx
            )
    print(f" {len(mapping):,} unique hostnames → {len(set(mapping.values())):,} providers")

    con.register("_mx_provider_map", pa.table({
        "mx_server": pa.array(list(mapping.keys())),
        "provider":  pa.array(list(mapping.values())),
    }))

    limit = f"LIMIT {top_n}" if top_n > 0 else ""
    rows = con.execute(f"""
        WITH pairs AS (
            SELECT DISTINCT
                m.provider,
                r.domain
            FROM (
                SELECT domain,
                       lower(rtrim(regexp_replace(mx_val, '^\\d+\\s+', ''), '.')) AS mx_server
                FROM _mx_raw
                WHERE mx_val IS NOT NULL AND mx_val != ''
            ) r
            JOIN _mx_provider_map m USING (mx_server)
        ),
        ranked AS (
            SELECT provider, COUNT(*) AS domains
            FROM pairs
            GROUP BY provider
            HAVING domains >= {min_count}
            ORDER BY domains DESC
            {limit}
        )
        SELECT
            row_number() OVER () AS rank,
            provider,
            printf('%,d', domains) AS domains,
            printf('%.2f%%', domains * 100.0 / {total}) AS pct
        FROM ranked
    """).fetchall()
    print_table(rows, ["rank", "mx_provider", "domains", "pct"])
    print(f"\n  {len(rows)} entries shown  |  total distinct domains with MX: {total:,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report historically most common MX servers (monthly S3 samples, 2016-present)."
    )
    parser.add_argument(
        "--source", choices=["toplists", "zonefiles", "both"], default="both",
        help="Which source(s) to query (default: both)",
    )
    parser.add_argument(
        "--top", type=int, default=1000, metavar="N",
        help="Top N entries to show; 0 = all above --min-count (default: 1000)",
    )
    parser.add_argument(
        "--min-count", type=int, default=10, metavar="M",
        help="Hide entries seen in fewer than M domains (default: 10)",
    )
    parser.add_argument(
        "--output", metavar="FILE",
        help="Write report to this file (default: auto-named in results/top_mx_historical/)",
    )
    args = parser.parse_args()

    dates = sample_dates()
    src_pairs = sources_for(args.source)
    if not src_pairs:
        print("No sources selected.", file=sys.stderr)
        sys.exit(1)

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = Path(args.output) if args.output else (
        _REPO_ROOT / 'results' / 'top_mx_historical' / f'top_mx_historical_{args.source}_{ts}.txt'
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_file = open(out_path, 'w')

    _orig_print = builtins.print

    def tee(*a, **kw):
        _orig_print(*a, **kw)
        end = kw.get('end', '\n')
        file_kw = {k: v for k, v in kw.items() if k != 'flush'}
        file_kw['file'] = out_file
        file_kw['end'] = end
        _orig_print(*a, **file_kw)

    builtins.print = tee

    date_range = f"{dates[0]} to {dates[-1]}"
    top_label = str(args.top) if args.top > 0 else "all"
    print(f"\nSource        : {args.source}")
    print(f"Date range    : {date_range}  ({len(dates)} sample months)")
    print(f"Sources/month : {', '.join(f'{b}/{s}' for b, s in src_pairs)}")
    print(f"Top           : {top_label}  |  min-count: {args.min_count}")
    print(f"\nExpected runtime: ~25-50 min for --source both")

    con, tmp_db = setup_connection()
    t0 = time.time()

    try:
        print("\n--- Accumulating S3 snapshots ---")
        loaded, skipped = accumulate(con, dates, src_pairs)

        total_rows = con.execute("SELECT COUNT(*) FROM _mx_raw").fetchone()[0]
        print(f"\nAccumulated {total_rows:,} raw rows  |  {loaded} files loaded, {skipped} skipped")

        print("\nCounting distinct MX domains ...", end="", flush=True)
        total_mx = count_mx_domains(con)
        print(f" {total_mx:,}")

        section(f"TOP MX SERVERS  (source={args.source}, historical={dates[0].year}-{dates[-1].year}, top={top_label}, min={args.min_count})")
        mx_analysis(con, args.top, args.min_count, total_mx)

        section(f"TOP MX PROVIDERS  (source={args.source}, historical={dates[0].year}-{dates[-1].year}, top={top_label}, min={args.min_count})")
        mx_provider_analysis(con, args.top, args.min_count, total_mx)

        builtins.print = _orig_print
        elapsed = time.time() - t0
        print(f"\nTotal time: {elapsed:.1f}s  ({elapsed/60:.1f} min)")
        out_file.close()
        print(f"Report written to {out_path}")
    finally:
        con.close()
        if os.path.exists(tmp_db):
            os.remove(tmp_db)
        wal = tmp_db + '.wal'
        if os.path.exists(wal):
            os.remove(wal)


if __name__ == "__main__":
    main()
