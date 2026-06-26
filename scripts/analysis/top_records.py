#!/usr/bin/env python3
"""
Report the most common MX servers across the event store.

Uses state.parquet (current/last-known values) rather than the event store,
so counts reflect actual prevalence rather than change frequency.

Usage:
  python scripts/analysis/top_records.py [--source {toplists,zonefiles,both}]
                                          [--top N] [--min-count M]
"""

import argparse
import datetime
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import tldextract

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


def count_mx_domains(con: duckdb.DuckDBPyConnection, pl: str) -> int:
    return con.execute(f"""
        SELECT COUNT(DISTINCT domain)
        FROM read_parquet({pl})
        WHERE query_type = 'MX' AND value IS NOT NULL AND len(value) > 0
    """).fetchone()[0]


def mx_analysis(
    con: duckdb.DuckDBPyConnection,
    pl: str,
    top_n: int,
    min_count: int,
    total: int,
) -> None:
    limit = f"LIMIT {top_n}" if top_n > 0 else ""
    rows = con.execute(f"""
        WITH ranked AS (
            SELECT
                mx_server,
                COUNT(DISTINCT domain) AS domains
            FROM (
                SELECT
                    domain,
                    lower(rtrim(regexp_replace(unnest(value), '^\\d+\\s+', ''), '.')) AS mx_server
                FROM read_parquet({pl})
                WHERE query_type = 'MX' AND value IS NOT NULL AND len(value) > 0
            )
            WHERE mx_server != ''
            GROUP BY mx_server
            HAVING domains >= {min_count}
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
    print(f"\n  {len(rows)} entries shown  |  total domains with MX: {total:,}")


def mx_provider_analysis(
    con: duckdb.DuckDBPyConnection,
    pl: str,
    top_n: int,
    min_count: int,
    total: int,
) -> None:
    print("  Building MX provider map via tldextract ...", end="", flush=True)
    unique_mx = con.execute(f"""
        SELECT DISTINCT
            lower(rtrim(regexp_replace(unnest(value), '^\\d+\\s+', ''), '.')) AS mx_server
        FROM read_parquet({pl})
        WHERE query_type = 'MX' AND value IS NOT NULL AND len(value) > 0
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
        WITH ranked AS (
            SELECT
                m.provider,
                COUNT(DISTINCT u.domain) AS domains
            FROM (
                SELECT
                    domain,
                    lower(rtrim(regexp_replace(unnest(value), '^\\d+\\s+', ''), '.')) AS mx_server
                FROM read_parquet({pl})
                WHERE query_type = 'MX' AND value IS NOT NULL AND len(value) > 0
            ) u
            JOIN _mx_provider_map m USING (mx_server)
            WHERE u.mx_server != ''
            GROUP BY m.provider
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
    print(f"\n  {len(rows)} entries shown  |  total domains with MX: {total:,}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report the most common MX servers in the event store."
    )
    parser.add_argument(
        "--source", choices=["toplists", "zonefiles", "both"], default="both",
        help="Which state file(s) to use (default: both)",
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
        help="Write report to this file (default: auto-named in results/top_mx/)",
    )
    args = parser.parse_args()

    paths = resolve_paths(args.source)
    if not paths:
        print("No state.parquet files found. Run the ingest pipeline first.", file=sys.stderr)
        sys.exit(1)

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = Path(args.output) if args.output else (
        _REPO_ROOT / 'results' / 'top_mx' / f'top_mx_{args.source}_{ts}.txt'
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_file = open(out_path, 'w')

    _orig_print = print

    def tee(*a, **kw):
        _orig_print(*a, **kw)
        end = kw.get('end', '\n')
        file_kw = {k: v for k, v in kw.items() if k != 'flush'}
        file_kw['file'] = out_file
        file_kw['end'] = end
        _orig_print(*a, **file_kw)

    import builtins
    builtins.print = tee

    pl = plist_sql(paths)
    top_label = str(args.top) if args.top > 0 else "all"
    print(f"\nSource : {args.source}")
    print(f"Files  : {', '.join(str(p) for p in paths)}")
    print(f"Top    : {top_label}  |  min-count: {args.min_count}")

    con = duckdb.connect()
    t0 = time.time()

    print("\nCounting MX domains ...", end="", flush=True)
    total_mx = count_mx_domains(con, pl)
    print(f" {total_mx:,}")

    section(f"TOP MX SERVERS  (source={args.source}, top={top_label}, min={args.min_count})")
    mx_analysis(con, pl, args.top, args.min_count, total_mx)

    section(f"TOP MX PROVIDERS  (source={args.source}, top={top_label}, min={args.min_count})")
    mx_provider_analysis(con, pl, args.top, args.min_count, total_mx)

    builtins.print = _orig_print
    print(f"\nTotal time: {time.time() - t0:.1f}s")
    out_file.close()
    print(f"Report written to {out_path}")


if __name__ == "__main__":
    main()
