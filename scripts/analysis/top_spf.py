#!/usr/bin/env python3
"""
Report most common SPF mechanisms across the event store, grouped by type.

Uses state.parquet (current/last-known values) rather than the event store,
so counts reflect actual prevalence rather than change frequency.

Usage:
  python scripts/analysis/top_spf.py [--source {toplists,zonefiles,both}]
                                      [--top N] [--min-count M]
"""

import argparse
import datetime
import sys
import time
from pathlib import Path

import duckdb

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

BASE = Path(__file__).resolve().parent.parent.parent / "data"
SOURCES = {
    "toplists": BASE / "toplists" / "state.parquet",
    "zonefiles": BASE / "zonefiles" / "state.parquet",
}


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


def plist_sql(paths: list[Path]) -> str:
    return "[" + ", ".join(f"'{p}'" for p in paths) + "]"


def resolve_paths(source: str) -> list[Path]:
    names = ["toplists", "zonefiles"] if source == "both" else [source]
    found = []
    for name in names:
        p = SOURCES[name]
        if p.exists():
            found.append(p)
        else:
            print(f"  WARNING: {p} not found, skipping '{name}'", file=sys.stderr)
    return found


def count_spf_domains(con: duckdb.DuckDBPyConnection, pl: str) -> int:
    return con.execute(f"""
        SELECT COUNT(DISTINCT domain) FROM (
            SELECT DISTINCT domain
            FROM (
                SELECT domain, trim(both '"' FROM unnest(value)) AS entry
                FROM read_parquet({pl})
                WHERE query_type = 'TXT' AND value IS NOT NULL AND len(value) > 0
            )
            WHERE entry LIKE 'v=spf1%'
        )
    """).fetchone()[0]


def create_spf_view(con: duckdb.DuckDBPyConnection, pl: str) -> None:
    """Tokenize SPF records and classify each token by mechanism type."""
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _spf_tokens AS
        WITH raw AS (
            SELECT domain, trim(both '"' FROM unnest(value)) AS entry
            FROM read_parquet({pl})
            WHERE query_type = 'TXT' AND value IS NOT NULL AND len(value) > 0
        ),
        spf AS (
            SELECT domain, entry FROM raw WHERE entry LIKE 'v=spf1%'
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
            -- extracted value after the mechanism keyword (NULL for bare a/mx/ptr)
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
        SELECT
            mtype,
            COUNT(DISTINCT domain) AS domains,
            printf('%.2f%%', COUNT(DISTINCT domain) * 100.0 / {total}) AS pct
        FROM _spf_tokens
        WHERE mtype IS NOT NULL
        GROUP BY mtype
        ORDER BY domains DESC
    """).fetchall()
    print_table(rows, ["mechanism_type", "domains", "pct"])
    print(f"\n  total domains with SPF: {total:,}")


def top_by_type(
    con: duckdb.DuckDBPyConnection,
    mtype: str,
    top_n: int,
    min_count: int,
    total: int,
) -> None:
    limit = f"LIMIT {top_n}" if top_n > 0 else ""
    rows = con.execute(f"""
        WITH ranked AS (
            SELECT
                mvalue AS value,
                COUNT(DISTINCT domain) AS domains
            FROM _spf_tokens
            WHERE mtype = '{mtype}' AND mvalue IS NOT NULL AND mvalue != ''
            GROUP BY mvalue
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
    print(f"\n  {len(rows)} entries shown  |  total domains with SPF: {total:,}")


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
    print(f"\n  total domains with SPF: {total:,}")


def top_spf_strings(
    con: duckdb.DuckDBPyConnection,
    pl: str,
    top_n: int,
    min_count: int,
    total: int,
) -> None:
    cap = min(top_n, 50) if top_n > 0 else 50
    rows = con.execute(f"""
        WITH ranked AS (
            SELECT
                entry AS spf_string,
                COUNT(DISTINCT domain) AS domains
            FROM (
                SELECT domain, trim(both '"' FROM unnest(value)) AS entry
                FROM read_parquet({pl})
                WHERE query_type = 'TXT' AND value IS NOT NULL AND len(value) > 0
            )
            WHERE entry LIKE 'v=spf1%'
            GROUP BY entry
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
    print(f"\n  {len(rows)} entries shown{note}  |  total domains with SPF: {total:,}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report most common SPF mechanisms in the event store, grouped by type."
    )
    parser.add_argument(
        "--source", choices=["toplists", "zonefiles", "both"], default="both",
        help="Which state file(s) to use (default: both)",
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
        help="Write report to this file (default: auto-named in results/top_spf/)",
    )
    args = parser.parse_args()

    paths = resolve_paths(args.source)
    if not paths:
        print("No state.parquet files found. Run the ingest pipeline first.", file=sys.stderr)
        sys.exit(1)

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = Path(args.output) if args.output else (
        _REPO_ROOT / 'results' / 'top_spf' / f'top_spf_{args.source}_{ts}.txt'
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_file = open(out_path, 'w')

    import builtins
    _orig_print = builtins.print

    def tee(*a, **kw):
        _orig_print(*a, **kw)
        end = kw.get('end', '\n')
        file_kw = {k: v for k, v in kw.items() if k != 'flush'}
        file_kw['file'] = out_file
        file_kw['end'] = end
        _orig_print(*a, **file_kw)

    builtins.print = tee

    pl = plist_sql(paths)
    top_label = str(args.top) if args.top > 0 else "all"
    print(f"\nSource : {args.source}")
    print(f"Files  : {', '.join(str(p) for p in paths)}")
    print(f"Top    : {top_label}  |  min-count: {args.min_count}")

    con = duckdb.connect()
    t0 = time.time()

    print("\nCounting SPF domains ...", end="", flush=True)
    total_spf = count_spf_domains(con, pl)
    print(f" {total_spf:,}")

    print("Building SPF token view ...", end="", flush=True)
    create_spf_view(con, pl)
    print(" done")

    section(f"MECHANISM TYPE DISTRIBUTION  (source={args.source})")
    mechanism_type_distribution(con, total_spf)

    section(f"TOP include: TARGETS  (source={args.source}, top={top_label}, min={args.min_count})")
    top_by_type(con, "include", args.top, args.min_count, total_spf)

    section(f"TOP redirect= TARGETS  (source={args.source}, top={top_label}, min={args.min_count})")
    top_by_type(con, "redirect", args.top, args.min_count, total_spf)

    section(f"TOP ip4: RANGES  (source={args.source}, top={top_label}, min={args.min_count})")
    top_by_type(con, "ip4", args.top, args.min_count, total_spf)

    section(f"ALL QUALIFIER DISTRIBUTION  (source={args.source})")
    all_qualifier_distribution(con, total_spf)

    section(f"TOP FULL SPF STRINGS  (source={args.source}, top=min(50,{top_label}), min={args.min_count})")
    top_spf_strings(con, pl, args.top, args.min_count, total_spf)

    builtins.print = _orig_print
    print(f"\nTotal time: {time.time() - t0:.1f}s")
    out_file.close()
    print(f"Report written to {out_path}")


if __name__ == "__main__":
    main()
