#!/usr/bin/env python3
"""
Report historically most common SPF mechanisms across monthly S3 snapshots (2016-present).

Queries OpenINTEL S3 directly, sampling the 15th of each month from January 2016
to today. Captures records not present in current state.parquet (retired services,
changed policies, etc.).

Usage:
  python scripts/analysis/top_spf_historical.py [--source {toplists,zonefiles,both}]
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
# Output helpers  (verbatim from top_spf.py)
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
    tmp = str(work_dir / f'spf_hist_{os.getpid()}.duckdb')
    con = duckdb.connect(tmp)
    con.execute("SET memory_limit='14GB'")
    con.execute(f"SET temp_directory='{work_dir}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")
    _setup_secrets(con)
    con.execute("""
        CREATE TABLE _spf_raw (
            domain    VARCHAR,
            spf_text  VARCHAR,
            PRIMARY KEY (domain, spf_text)
        )
    """)
    return con, tmp


def try_ingest(con: duckdb.DuckDBPyConnection, basis: str, source: str,
               d: datetime.date) -> tuple[bool, int]:
    path = _s3_path(basis, source, d)
    try:
        before = con.execute("SELECT COUNT(*) FROM _spf_raw").fetchone()[0]
        con.execute(f"""
            INSERT OR IGNORE INTO _spf_raw
            SELECT DISTINCT lower(query_name), txt_text
            FROM read_parquet('{path}')
            WHERE query_type = 'TXT'
              AND txt_text IS NOT NULL
              AND (txt_text LIKE 'v=spf1%' OR txt_text LIKE '"v=spf1%')
        """)
        after = con.execute("SELECT COUNT(*) FROM _spf_raw").fetchone()[0]
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
# Analysis  (adapted from top_spf.py — reads _spf_raw instead of read_parquet)
# ---------------------------------------------------------------------------

def count_spf_domains(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT domain FROM _spf_raw)"
    ).fetchone()[0]


def create_spf_view(con: duckdb.DuckDBPyConnection) -> None:
    """Tokenize and classify SPF mechanisms from accumulated raw data."""
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _spf_tokens AS
        WITH spf AS (
            SELECT domain, trim(both '"' FROM spf_text) AS entry
            FROM _spf_raw
            WHERE spf_text IS NOT NULL
        ),
        tokens AS (
            SELECT domain, lower(trim(unnest(string_split(entry, ' ')))) AS token
            FROM spf
        )
        SELECT
            domain,
            token,
            CASE
                WHEN token LIKE 'include:%'                                        THEN 'include'
                WHEN token LIKE 'redirect=%'                                       THEN 'redirect'
                WHEN token LIKE 'ip4:%'                                            THEN 'ip4'
                WHEN token LIKE 'ip6:%'                                            THEN 'ip6'
                WHEN token LIKE 'a:%' OR token IN ('a','+a','-a','~a','?a')       THEN 'a'
                WHEN token LIKE 'mx:%' OR token IN ('mx','+mx','-mx','~mx','?mx') THEN 'mx'
                WHEN token LIKE 'exists:%'                                         THEN 'exists'
                WHEN token LIKE 'ptr:%' OR token IN ('ptr','+ptr','-ptr','~ptr','?ptr') THEN 'ptr'
                WHEN token IN ('-all','~all','+all','?all','all')                  THEN 'all'
                ELSE 'other'
            END AS mtype,
            CASE
                WHEN token LIKE 'include:%'  THEN substr(token, 9)
                WHEN token LIKE 'redirect=%' THEN substr(token, 10)
                WHEN token LIKE 'ip4:%'      THEN substr(token, 5)
                WHEN token LIKE 'ip6:%'      THEN substr(token, 5)
                WHEN token LIKE 'a:%'        THEN substr(token, 3)
                WHEN token LIKE 'mx:%'       THEN substr(token, 4)
                ELSE NULL
            END AS mvalue
        FROM tokens
        WHERE token != '' AND token != 'v=spf1'
    """)


def mechanism_type_distribution(con: duckdb.DuckDBPyConnection, total: int) -> None:
    rows = con.execute(f"""
        WITH pairs AS (
            SELECT DISTINCT mtype, domain FROM _spf_tokens WHERE mtype IS NOT NULL
        )
        SELECT mtype, COUNT(*) AS domains,
               printf('%.2f%%', COUNT(*) * 100.0 / {total}) AS pct
        FROM pairs
        GROUP BY mtype
        ORDER BY domains DESC
    """).fetchall()
    print_table(rows, ["mechanism_type", "domains", "pct"])
    print(f"\n  total distinct domains with SPF: {total:,}")


def top_by_type(con: duckdb.DuckDBPyConnection, mtype: str, top_n: int,
                min_count: int, total: int) -> None:
    limit = f"LIMIT {top_n}" if top_n > 0 else ""
    rows = con.execute(f"""
        WITH pairs AS (
            SELECT DISTINCT mvalue AS value, domain
            FROM _spf_tokens
            WHERE mtype = '{mtype}' AND mvalue IS NOT NULL AND mvalue != ''
        ),
        ranked AS (
            SELECT value, COUNT(*) AS domains
            FROM pairs
            GROUP BY value
            HAVING domains >= {min_count}
            ORDER BY domains DESC
            {limit}
        )
        SELECT
            row_number() OVER () AS rank,
            value,
            printf('%,d', domains) AS domains,
            printf('%.2f%%', domains * 100.0 / {total}) AS pct
        FROM ranked
    """).fetchall()
    print_table(rows, ["rank", "value", "domains", "pct"])
    print(f"\n  {len(rows)} entries shown  |  total distinct domains with SPF: {total:,}")


def all_qualifier_distribution(con: duckdb.DuckDBPyConnection, total: int) -> None:
    rows = con.execute(f"""
        WITH has_all AS (
            SELECT DISTINCT domain, token FROM _spf_tokens WHERE mtype = 'all'
        ),
        no_all AS (
            SELECT DISTINCT domain FROM _spf_tokens
            WHERE domain NOT IN (SELECT domain FROM has_all)
        )
        SELECT token AS qualifier, COUNT(*) AS domains,
               printf('%.2f%%', COUNT(*) * 100.0 / {total}) AS pct
        FROM has_all
        GROUP BY token
        UNION ALL
        SELECT '(no all)' AS qualifier, COUNT(*) AS domains,
               printf('%.2f%%', COUNT(*) * 100.0 / {total}) AS pct
        FROM no_all
        ORDER BY domains DESC
    """).fetchall()
    print_table(rows, ["qualifier", "domains", "pct"])
    print(f"\n  total distinct domains with SPF: {total:,}")


def top_spf_strings(con: duckdb.DuckDBPyConnection, top_n: int, min_count: int,
                    total: int) -> None:
    cap = min(top_n, 50) if top_n > 0 else 50
    rows = con.execute(f"""
        WITH pairs AS (
            SELECT DISTINCT trim(both '"' FROM spf_text) AS spf_string, domain
            FROM _spf_raw
        ),
        ranked AS (
            SELECT spf_string, COUNT(*) AS domains
            FROM pairs
            GROUP BY spf_string
            HAVING domains >= {min_count}
            ORDER BY domains DESC
            LIMIT {cap}
        )
        SELECT
            row_number() OVER () AS rank,
            spf_string,
            printf('%,d', domains) AS domains,
            printf('%.2f%%', domains * 100.0 / {total}) AS pct
        FROM ranked
    """).fetchall()
    print_table(rows, ["rank", "spf_string", "domains", "pct"])
    note = "  (capped at 50)" if top_n == 0 or top_n > 50 else ""
    print(f"\n  {len(rows)} entries shown{note}  |  total distinct domains with SPF: {total:,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report historically most common SPF mechanisms (monthly S3 samples, 2016-present)."
    )
    parser.add_argument(
        "--source", choices=["toplists", "zonefiles", "both"], default="both",
        help="Which source(s) to query (default: both)",
    )
    parser.add_argument(
        "--top", type=int, default=1000, metavar="N",
        help="Top N entries per section; 0 = all above --min-count (default: 1000)",
    )
    parser.add_argument(
        "--min-count", type=int, default=10, metavar="M",
        help="Hide entries seen in fewer than M domains (default: 10)",
    )
    parser.add_argument(
        "--output", metavar="FILE",
        help="Write report to this file (default: auto-named in results/top_spf_historical/)",
    )
    args = parser.parse_args()

    dates = sample_dates()
    src_pairs = sources_for(args.source)
    if not src_pairs:
        print("No sources selected.", file=sys.stderr)
        sys.exit(1)

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = Path(args.output) if args.output else (
        _REPO_ROOT / 'results' / 'top_spf_historical' / f'top_spf_historical_{args.source}_{ts}.txt'
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

        total_rows = con.execute("SELECT COUNT(*) FROM _spf_raw").fetchone()[0]
        print(f"\nAccumulated {total_rows:,} raw rows  |  {loaded} files loaded, {skipped} skipped")

        print("Counting distinct SPF domains ...", end="", flush=True)
        total_spf = count_spf_domains(con)
        print(f" {total_spf:,}")

        print("Building SPF token view ...", end="", flush=True)
        create_spf_view(con)
        print(" done")

        yr0, yr1 = dates[0].year, dates[-1].year

        section(f"MECHANISM TYPE DISTRIBUTION  (source={args.source}, historical={yr0}-{yr1})")
        mechanism_type_distribution(con, total_spf)

        section(f"TOP include: TARGETS  (source={args.source}, historical={yr0}-{yr1}, top={top_label}, min={args.min_count})")
        top_by_type(con, "include", args.top, args.min_count, total_spf)

        section(f"TOP redirect= TARGETS  (source={args.source}, historical={yr0}-{yr1}, top={top_label}, min={args.min_count})")
        top_by_type(con, "redirect", args.top, args.min_count, total_spf)

        section(f"TOP ip4: RANGES  (source={args.source}, historical={yr0}-{yr1}, top={top_label}, min={args.min_count})")
        top_by_type(con, "ip4", args.top, args.min_count, total_spf)

        section(f"ALL QUALIFIER DISTRIBUTION  (source={args.source}, historical={yr0}-{yr1})")
        all_qualifier_distribution(con, total_spf)

        section(f"TOP FULL SPF STRINGS  (source={args.source}, historical={yr0}-{yr1}, top=min(50,{top_label}), min={args.min_count})")
        top_spf_strings(con, args.top, args.min_count, total_spf)

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
