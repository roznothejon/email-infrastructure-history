#!/usr/bin/env python3
"""
Track monthly adoption of SPF include categories and top-N specific includes
across the event store.

Reconstructs per-domain SPF state at the end of each calendar month from
change events, then counts how many domains use each SPF include/category.

Outputs two Parquet files (for plotting) and a human-readable .txt report.

Usage:
  python scripts/analysis/spf_adoption_monthly.py [--source {toplists,zonefiles,both}]
                                                    [--top-includes N]
                                                    [--output-dir DIR]

Expected runtime: 10-30 min depending on source and hardware.
"""

import argparse
import builtins
import datetime
import glob
import json
import os
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa

_REPO = Path(__file__).resolve().parent.parent.parent
DATA  = _REPO / "data"
SPF_MAP_PATH = DATA / "mappings" / "spf_providers.json"


def _discover_sources() -> dict[str, Path]:
    found = {}
    for p in sorted(DATA.iterdir()):
        if p.is_dir() and (p / "events").is_dir() and p.name != "mappings":
            found[p.name] = p / "events"
    return found


SOURCES: dict[str, Path] = _discover_sources()

N_BUCKETS = 256
MONTH_SERIES_START = "2016-01-01"


# ---------------------------------------------------------------------------
# Helpers
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
            is_num = i > 0 and v.replace(",", "").replace(".", "").replace("%", "").lstrip("-").isdigit()
            parts.append(v.rjust(widths[i]) if is_num else v.ljust(widths[i]))
        print("  " + "  ".join(parts))


def bucket_paths(bucket: int, selected: list[str]) -> list[str]:
    """Return existing parquet glob paths for this bucket across requested sources."""
    paths = []
    for name in selected:
        pattern = str(SOURCES[name] / f"bucket={bucket}" / "*.parquet")
        if glob.glob(pattern):
            paths.append(pattern)
    return paths


def month_series_end(sources: list[str]) -> str:
    """First-of-month for the earliest `last_date` across the selected sources'
    ingest_log.json — never run the series past what was actually ingested."""
    last_dates = []
    for name in sources:
        log_path = SOURCES[name].parent / "ingest_log.json"
        if log_path.exists():
            with open(log_path) as f:
                last_dates.append(json.load(f)["last_date"])
    if not last_dates:
        today = datetime.date.today()
        return datetime.date(today.year, today.month, 1).isoformat()
    earliest = min(datetime.date.fromisoformat(d) for d in last_dates)
    return datetime.date(earliest.year, earliest.month, 1).isoformat()


# ---------------------------------------------------------------------------
# Accumulator setup
# ---------------------------------------------------------------------------

def setup_accumulator(tmp_path: str, work_dir: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(tmp_path)
    con.execute("SET memory_limit='14GB'")
    con.execute(f"SET temp_directory='{work_dir}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")
    con.execute("""
        CREATE TABLE IF NOT EXISTS _inc_monthly (
            month      DATE,
            inc_domain VARCHAR,
            n          BIGINT,
            PRIMARY KEY (month, inc_domain)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _spf_total (
            month DATE,
            n     BIGINT,
            PRIMARY KEY (month)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _dom_total (
            month DATE,
            n     BIGINT,
            PRIMARY KEY (month)
        )
    """)
    return con


# ---------------------------------------------------------------------------
# Per-bucket processing
# ---------------------------------------------------------------------------

def process_bucket(con: duckdb.DuckDBPyConnection,
                   bucket: int,
                   paths: list[str],
                   month_start: str,
                   month_end: str) -> tuple[int, int]:
    """Compute monthly (inc_domain, count) and (total_spf, count) for one bucket."""

    plist = "[" + ", ".join(f"'{p}'" for p in paths) + "]"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _bucket_counts AS
        WITH events AS (
            SELECT domain, measurement_date, value,
                   LEAD(measurement_date) OVER (
                       PARTITION BY domain ORDER BY measurement_date
                   ) AS next_date
            FROM read_parquet({plist})
            WHERE query_type = 'TXT'
        ),
        events_all AS (
            SELECT domain, measurement_date, value,
                   LEAD(measurement_date) OVER (
                       PARTITION BY domain, query_type ORDER BY measurement_date
                   ) AS next_date
            FROM read_parquet({plist})
        ),
        month_series AS (
            SELECT m::DATE AS month_start
            FROM generate_series(
                DATE '{month_start}',
                DATE '{month_end}',
                INTERVAL '1 month'
            ) t(m)
        ),
        -- Any domain with a live MX or TXT record that month = part of the measured universe
        active_all AS (
            SELECT ms.month_start, e.domain
            FROM events_all e
            JOIN month_series ms
              ON DATE_TRUNC('month', e.measurement_date) <= ms.month_start
             AND (e.next_date IS NULL
                  OR DATE_TRUNC('month', e.next_date) > ms.month_start)
            WHERE e.value IS NOT NULL
        ),
        dom_totals AS (
            SELECT month_start, COUNT(DISTINCT domain) AS n
            FROM active_all
            GROUP BY month_start
        ),
        -- Carry each event forward until the next event fires
        active AS (
            SELECT ms.month_start, e.domain, e.value
            FROM events e
            JOIN month_series ms
              ON DATE_TRUNC('month', e.measurement_date) <= ms.month_start
             AND (e.next_date IS NULL
                  OR DATE_TRUNC('month', e.next_date) > ms.month_start)
            WHERE e.value IS NOT NULL
        ),
        -- Unnest value list and strip surrounding quotes stored by the pipeline
        spf_entries AS (
            SELECT month_start, domain,
                   trim(BOTH '"' FROM unnest(value)) AS spf_entry
            FROM active
        ),
        -- Keep only SPF records; dedup so one domain/month counts once per SPF string
        spf_filtered AS (
            SELECT DISTINCT month_start, domain, spf_entry
            FROM spf_entries
            WHERE spf_entry LIKE 'v=spf1%'
        ),
        -- Total SPF-using domains per month
        spf_totals AS (
            SELECT month_start, COUNT(DISTINCT domain) AS n
            FROM spf_filtered
            GROUP BY month_start
        ),
        -- Extract include: targets (includes never contain spaces, so [^ ]+ is exact)
        includes_raw AS (
            SELECT month_start, domain,
                   lower(trim(
                       unnest(regexp_extract_all(spf_entry, 'include:([^ ]+)', 1))
                   )) AS inc
            FROM spf_filtered
            WHERE spf_entry LIKE '%include:%'
        ),
        -- Aggregate: distinct domains per (month, include)
        inc_counts AS (
            SELECT month_start, inc, COUNT(DISTINCT domain) AS n
            FROM includes_raw
            WHERE inc != ''
            GROUP BY month_start, inc
        )
        SELECT 'inc' AS rtype, month_start, inc AS key, n FROM inc_counts
        UNION ALL
        SELECT 'tot' AS rtype, month_start, '' AS key, n FROM spf_totals
        UNION ALL
        SELECT 'dom' AS rtype, month_start, '' AS key, n FROM dom_totals
    """)

    n_inc = con.execute(
        "SELECT COUNT(*) FROM _bucket_counts WHERE rtype = 'inc'"
    ).fetchone()[0]
    n_tot = con.execute(
        "SELECT COUNT(*) FROM _bucket_counts WHERE rtype = 'tot'"
    ).fetchone()[0]
    n_dom = con.execute(
        "SELECT COUNT(*) FROM _bucket_counts WHERE rtype = 'dom'"
    ).fetchone()[0]

    if n_inc > 0:
        con.execute("""
            INSERT INTO _inc_monthly (month, inc_domain, n)
            SELECT month_start, key, n FROM _bucket_counts WHERE rtype = 'inc'
            ON CONFLICT (month, inc_domain) DO UPDATE SET n = n + excluded.n
        """)

    if n_tot > 0:
        con.execute("""
            INSERT INTO _spf_total (month, n)
            SELECT month_start, n FROM _bucket_counts WHERE rtype = 'tot'
            ON CONFLICT (month) DO UPDATE SET n = n + excluded.n
        """)

    if n_dom > 0:
        con.execute("""
            INSERT INTO _dom_total (month, n)
            SELECT month_start, n FROM _bucket_counts WHERE rtype = 'dom'
            ON CONFLICT (month) DO UPDATE SET n = n + excluded.n
        """)

    con.execute("DROP TABLE IF EXISTS _bucket_counts")
    return n_inc, n_tot


# ---------------------------------------------------------------------------
# Final aggregation
# ---------------------------------------------------------------------------

def build_spf_map_table(con: duckdb.DuckDBPyConnection) -> None:
    """Register spf_providers.json as a DuckDB Arrow table for category joins."""
    spf_map: dict = json.loads(SPF_MAP_PATH.read_text())
    domains   = list(spf_map.keys())
    providers = [v["provider"] for v in spf_map.values()]
    categories = [v.get("type", "") for v in spf_map.values()]
    tbl = pa.table({
        "inc_domain": pa.array(domains, type=pa.string()),
        "provider":   pa.array(providers, type=pa.string()),
        "category":   pa.array(categories, type=pa.string()),
    })
    con.register("_spf_map", tbl)


def write_overall_parquet(con: duckdb.DuckDBPyConnection, path: str) -> int:
    """Write (month, spf_domains, total_domains, pct) — overall SPF adoption vs all measured domains."""
    con.execute(f"""
        COPY (
            SELECT s.month,
                   s.n::BIGINT AS spf_domains,
                   d.n::BIGINT AS total_domains,
                   ROUND(s.n * 100.0 / NULLIF(d.n, 0), 4) AS pct
            FROM _spf_total s
            JOIN _dom_total d USING (month)
            ORDER BY s.month
        ) TO '{path}' (FORMAT PARQUET, CODEC 'ZSTD')
    """)
    return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]


def write_categories_parquet(con: duckdb.DuckDBPyConnection, path: str) -> int:
    """Write (month, category, domain_count, total_spf_domains, pct) to parquet."""
    con.execute(f"""
        COPY (
            WITH cat AS (
                SELECT i.month,
                       COALESCE(NULLIF(m.category, ''), 'unknown') AS category,
                       SUM(i.n) AS domain_count
                FROM _inc_monthly i
                LEFT JOIN _spf_map m USING (inc_domain)
                GROUP BY i.month, category
            )
            SELECT
                c.month,
                c.category,
                c.domain_count::BIGINT AS domain_count,
                t.n::BIGINT AS total_spf_domains,
                ROUND(c.domain_count * 100.0 / NULLIF(t.n, 0), 4) AS pct
            FROM cat c
            JOIN _spf_total t USING (month)
            ORDER BY c.month, c.domain_count DESC
        ) TO '{path}' (FORMAT PARQUET, CODEC 'ZSTD')
    """)
    return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]


def write_includes_parquet(con: duckdb.DuckDBPyConnection, path: str, top_n: int) -> int:
    """Write top-N includes by all-time total, with monthly rows, to parquet."""
    con.execute(f"""
        COPY (
            WITH top_incs AS (
                SELECT inc_domain
                FROM _inc_monthly
                GROUP BY inc_domain
                ORDER BY SUM(n) DESC
                LIMIT {top_n}
            )
            SELECT
                i.month,
                i.inc_domain,
                COALESCE(m.provider, i.inc_domain) AS provider,
                COALESCE(NULLIF(m.category, ''), 'unknown') AS category,
                i.n::BIGINT AS domain_count,
                t.n::BIGINT AS total_spf_domains,
                ROUND(i.n * 100.0 / NULLIF(t.n, 0), 4) AS pct
            FROM _inc_monthly i
            JOIN top_incs USING (inc_domain)
            JOIN _spf_total t USING (month)
            LEFT JOIN _spf_map m USING (inc_domain)
            ORDER BY i.month, i.n DESC
        ) TO '{path}' (FORMAT PARQUET, CODEC 'ZSTD')
    """)
    return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]


# ---------------------------------------------------------------------------
# Text report
# ---------------------------------------------------------------------------

def report_totals(con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute("""
        SELECT s.month,
               printf('%,d', s.n) AS spf_domains,
               printf('%,d', d.n) AS total_domains,
               printf('%.2f%%', ROUND(s.n * 100.0 / NULLIF(d.n, 0), 2)) AS spf_pct,
               printf('%+,d', s.n - LAG(s.n, 12) OVER (ORDER BY s.month)) AS yoy_change
        FROM _spf_total s
        JOIN _dom_total d USING (month)
        ORDER BY s.month
    """).fetchall()
    print_table(rows, ["month", "spf_domains", "total_domains", "spf_pct", "yoy_change"])
    print(f"\n  {len(rows)} months")


def report_categories(con: duckdb.DuckDBPyConnection) -> None:
    # Peak adoption per category, plus latest-month stats
    latest_month = con.execute("SELECT MAX(month) FROM _spf_total").fetchone()[0]
    rows = con.execute(f"""
        WITH cat AS (
            SELECT i.month,
                   COALESCE(NULLIF(m.category, ''), 'unknown') AS category,
                   SUM(i.n) AS n,
                   t.n AS total
            FROM _inc_monthly i
            LEFT JOIN _spf_map m USING (inc_domain)
            JOIN _spf_total t USING (month)
            GROUP BY i.month, category, total
        ),
        peak AS (
            SELECT category,
                   MAX(n) AS peak_n,
                   arg_max(month, n) AS peak_month,
                   arg_max(ROUND(n * 100.0 / NULLIF(total, 0), 2), n) AS peak_pct
            FROM cat GROUP BY category
        ),
        latest AS (
            SELECT category, n AS latest_n,
                   ROUND(n * 100.0 / NULLIF(total, 0), 2) AS latest_pct
            FROM cat WHERE month = DATE '{latest_month}'
        )
        SELECT p.category,
               p.peak_month,
               printf('%,d', p.peak_n) AS peak_domains,
               printf('%.2f%%', p.peak_pct) AS peak_pct,
               printf('%,d', COALESCE(l.latest_n, 0)) AS latest_domains,
               printf('%.2f%%', COALESCE(l.latest_pct, 0)) AS latest_pct
        FROM peak p
        LEFT JOIN latest l USING (category)
        ORDER BY p.peak_n DESC
    """).fetchall()
    print(f"\n  (latest month: {latest_month})")
    print_table(rows, ["category", "peak_month", "peak_domains", "peak_pct",
                        "latest_domains", "latest_pct"])


def report_top_includes(con: duckdb.DuckDBPyConnection, top_n: int) -> None:
    latest_month = con.execute("SELECT MAX(month) FROM _spf_total").fetchone()[0]
    # Roughly "5 years ago" for a trend column
    trend_month = con.execute(f"""
        SELECT month FROM _spf_total
        WHERE month <= (DATE '{latest_month}' - INTERVAL '5 years')
        ORDER BY month DESC LIMIT 1
    """).fetchone()
    trend_month = trend_month[0] if trend_month else None

    trend_join = ""
    trend_col_sql = "NULL AS trend_n, NULL AS trend_pct"
    if trend_month:
        trend_join = f"""
            LEFT JOIN (
                SELECT i2.inc_domain,
                       i2.n AS trend_n,
                       ROUND(i2.n * 100.0 / NULLIF(t2.n, 0), 2) AS trend_pct
                FROM _inc_monthly i2
                JOIN _spf_total t2 ON t2.month = i2.month
                WHERE i2.month = DATE '{trend_month}'
            ) tr USING (inc_domain)
        """
        trend_col_sql = "tr.trend_n, tr.trend_pct"

    rows = con.execute(f"""
        WITH totals AS (
            SELECT inc_domain, SUM(n) AS total_all_time
            FROM _inc_monthly
            GROUP BY inc_domain
            ORDER BY total_all_time DESC
            LIMIT {top_n}
        ),
        latest AS (
            SELECT i.inc_domain,
                   i.n AS latest_n,
                   ROUND(i.n * 100.0 / NULLIF(t.n, 0), 2) AS latest_pct
            FROM _inc_monthly i
            JOIN _spf_total t ON t.month = i.month
            WHERE i.month = DATE '{latest_month}'
        )
        SELECT
            row_number() OVER (ORDER BY to2.total_all_time DESC) AS rank,
            to2.inc_domain,
            COALESCE(NULLIF(m.category, ''), 'unknown') AS category,
            printf('%,d', to2.total_all_time) AS total_domain_months,
            printf('%,d', COALESCE(la.latest_n, 0)) AS latest_domains,
            printf('%.2f%%', COALESCE(la.latest_pct, 0)) AS latest_pct
        FROM totals to2
        LEFT JOIN latest la USING (inc_domain)
        LEFT JOIN _spf_map m USING (inc_domain)
        ORDER BY to2.total_all_time DESC
    """).fetchall()

    trend_note = f"  (latest: {latest_month})"
    if trend_month:
        trend_note += f"  (trend base: {trend_month})"
    print(trend_note)
    print_table(rows, ["rank", "inc_domain", "category", "total_domain_months",
                        "latest_domains", "latest_pct"])
    print(f"\n  {len(rows)} entries shown  |  ranked by sum of monthly domain-counts")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monthly SPF include adoption from the event store."
    )
    all_sources = sorted(SOURCES.keys())
    parser.add_argument(
        "--source", nargs="+", default=all_sources,
        metavar="NAME",
        help=(
            f"Which source(s) to use (default: all discovered). "
            f"Available: {', '.join(all_sources)}"
        ),
    )
    parser.add_argument(
        "--top-includes", type=int, default=75, metavar="N",
        help="Track this many top includes in the includes output (default: 75)",
    )
    parser.add_argument(
        "--output-dir", metavar="DIR",
        help="Write outputs here (default: results/spf_adoption/)",
    )
    args = parser.parse_args()

    selected = args.source
    unknown = [s for s in selected if s not in SOURCES]
    if unknown:
        parser.error(
            f"Unknown source(s): {', '.join(unknown)}. "
            f"Available: {', '.join(sorted(SOURCES.keys()))}"
        )

    source_label = "+".join(selected)

    out_dir = Path(args.output_dir) if args.output_dir else (
        _REPO / "data" / "stats" / "spf_adoption"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    # Parquet files use fixed names so the app can read them without knowing the timestamp.
    # The text report keeps a timestamp for archival.
    txt_path     = out_dir / f"spf_adoption_{source_label}_{ts}.txt"
    overall_parquet = out_dir / "overall_monthly.parquet"
    cat_parquet  = out_dir / "categories_monthly.parquet"
    inc_parquet  = out_dir / "includes_monthly.parquet"

    txt_file = open(txt_path, "w")
    _orig_print = builtins.print

    def tee(*a, **kw):
        _orig_print(*a, **kw)
        end = kw.get("end", "\n")
        fkw = {k: v for k, v in kw.items() if k != "flush"}
        fkw["file"] = txt_file
        fkw["end"] = end
        _orig_print(*a, **fkw)

    builtins.print = tee

    m_end = month_series_end(selected)
    print(f"\nSPF Monthly Adoption Analysis")
    print(f"Sources        : {', '.join(selected)}")
    print(f"Month range    : {MONTH_SERIES_START} to {m_end}")
    print(f"Top includes   : {args.top_includes}")
    print(f"Output dir     : {out_dir}")
    print(f"Timestamp      : {ts}")

    work_dir = str(_REPO / "tmp")
    os.makedirs(work_dir, exist_ok=True)
    tmp_db = os.path.join(work_dir, f"spf_adoption_{os.getpid()}.duckdb")

    con = setup_accumulator(tmp_db, work_dir)
    t_start = time.time()

    try:
        # ---- Bucket loop ----
        print(f"\n--- Processing {N_BUCKETS} buckets ---")
        skipped = 0
        bucket_times: list[float] = []

        for bucket in range(N_BUCKETS):
            paths = bucket_paths(bucket, selected)
            if not paths:
                skipped += 1
                continue

            t0 = time.time()
            n_inc, n_tot = process_bucket(con, bucket, paths, MONTH_SERIES_START, m_end)
            elapsed = time.time() - t0
            bucket_times.append(elapsed)

            avg = sum(bucket_times) / len(bucket_times)
            remaining = (N_BUCKETS - bucket - 1) * avg
            eta_str = f"ETA {remaining/60:.1f}min" if remaining > 60 else f"ETA {remaining:.0f}s"
            _orig_print(
                f"  bucket {bucket:3d}/255  "
                f"{n_inc:6,} inc rows  {n_tot:4,} tot rows  "
                f"{elapsed:.1f}s  {eta_str}",
                flush=True,
            )

        total_elapsed = time.time() - t_start
        total_inc = con.execute("SELECT COUNT(*) FROM _inc_monthly").fetchone()[0]
        total_months = con.execute("SELECT COUNT(*) FROM _spf_total").fetchone()[0]
        unique_incs = con.execute("SELECT COUNT(DISTINCT inc_domain) FROM _inc_monthly").fetchone()[0]

        print(f"\nAccumulation complete: {total_elapsed/60:.1f} min")
        print(f"  Unique (month, inc_domain) pairs : {total_inc:,}")
        print(f"  Unique months with SPF data      : {total_months:,}")
        print(f"  Unique include domains seen      : {unique_incs:,}")
        if skipped:
            print(f"  Buckets skipped (no data)        : {skipped}")

        # ---- Build SPF map lookup ----
        print("\nJoining with provider mappings ...", end="", flush=True)
        build_spf_map_table(con)
        print(" done")

        # ---- Reports ----
        section("OVERALL SPF ADOPTION (% OF ALL MEASURED DOMAINS)")
        report_totals(con)

        section("CATEGORY ADOPTION — peak and latest")
        report_categories(con)

        section(f"TOP {args.top_includes} SPF INCLUDES — ranked by total domain-months")
        report_top_includes(con, args.top_includes)

        # ---- Save Parquet ----
        print(f"\nWriting Parquet outputs ...")

        overall_rows = write_overall_parquet(con, str(overall_parquet))
        print(f"  {overall_parquet.name}  ({overall_rows:,} rows)")

        cat_rows = write_categories_parquet(con, str(cat_parquet))
        print(f"  {cat_parquet.name}  ({cat_rows:,} rows)")

        inc_rows = write_includes_parquet(con, str(inc_parquet), args.top_includes)
        print(f"  {inc_parquet.name}  ({inc_rows:,} rows)")

        builtins.print = _orig_print
        print(f"\nTotal time: {(time.time() - t_start)/60:.1f} min")
        txt_file.close()
        print(f"Report : {txt_path}")
        print(f"Parquet: {overall_parquet}")
        print(f"Parquet: {cat_parquet}")
        print(f"Parquet: {inc_parquet}")

    except Exception:
        builtins.print = _orig_print
        raise
    finally:
        con.close()
        for f in [tmp_db, tmp_db + ".wal"]:
            if os.path.exists(f):
                os.remove(f)


if __name__ == "__main__":
    main()
