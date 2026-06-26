#!/usr/bin/env python3
"""
Analyzes the event store produced by preprocess_parquet_v2.py.

Computes:
  - Event type breakdown (appearances, disappearances, genuine changes)
  - Unique domain/registrable-domain counts
  - Churn: domains that disappear and reappear within N days
  - Ephemeral domains: active < 7 days
  - Grace period analysis: optimal suppression window
  - Additional: date range, events-per-day distribution, MX vs TXT split,
    top-1000 volatile domains, multi-churn domains

Usage:
  python scripts/analysis/dataset_metrics.py [--data-dir DATA_DIR] [--output FILE]
"""

import argparse
import datetime
import heapq
import time
from collections import defaultdict
from pathlib import Path

import duckdb

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


GRACE_CANDIDATES = [1, 2, 3, 5, 7, 10, 14, 21, 30]


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def step(msg: str) -> None:
    print(f"  >> {msg} ...", flush=True)


def pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "N/A"
    return f"{numerator / denominator * 100:.2f}%"


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute dataset metrics for the event store.")
    parser.add_argument("--data-dir", default="data/toplists", help="Root data directory (default: data/toplists)")
    parser.add_argument("--output", metavar="FILE",
                        help="Write report to this file (default: auto-named in results/dataset_metrics/)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_lines: list[str] = []

    def emit(line: str = "") -> None:
        print(line)
        out_lines.append(line)

    t0 = time.time()

    # ---------- Accumulators ----------
    # Each domain lives in exactly one bucket (bucketed by registrable_domain hash),
    # so per-bucket aggregates sum correctly to global totals.
    total_events = 0
    counts: dict[str, int] = defaultdict(int)

    unique_domains_total = 0
    unique_reg_total = 0
    qt_unique_domains: dict[str, int] = defaultdict(int)
    qt_unique_reg: dict[str, int] = defaultdict(int)
    qt_event_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    date_event_counts: dict = defaultdict(int)
    n_days_set: set = set()
    first_date = None
    last_date = None

    gap_map: dict[int, int] = defaultdict(int)        # gap_days -> count (disappear→reappear)
    span_map: dict[int, int] = defaultdict(int)       # span_days -> count (appear→disappear, capped at 30)
    total_pairs = 0
    ephemeral_pairs = 0
    ephemeral_unique = 0

    churn_distribution: dict[int, int] = defaultdict(int)  # n_disap -> domain,qt pair count

    # Min-heap of (total_events, domain, qt, changes, disappearances, appearances)
    volatile_heap: list = []

    # ---------- Per-bucket processing ----------
    n_buckets_processed = 0
    for bucket in range(256):
        bucket_dir = data_dir / "events" / f"bucket={bucket:03d}"
        if not bucket_dir.exists():
            continue
        parquet_files = list(bucket_dir.glob("*.parquet"))
        if not parquet_files:
            continue

        n_buckets_processed += 1
        if n_buckets_processed % 32 == 1:
            elapsed = time.time() - t0
            print(f"  bucket {bucket:03d}/255  ({elapsed:.0f}s elapsed)", flush=True)

        bucket_glob = str(bucket_dir / "*.parquet")

        con = duckdb.connect()
        con.execute("SET memory_limit='4GB'")
        con.execute("SET enable_progress_bar=false")

        # Materialize classified for this bucket only (small — ~1/256 of dataset).
        # LEAD/LAG are correct because all events for a given domain+query_type
        # are guaranteed to land in the same bucket.
        con.execute(f"""
            CREATE TABLE classified AS
            SELECT
                domain,
                registrable_domain,
                query_type,
                measurement_date,
                CASE
                    WHEN prev_value IS NULL AND value IS NOT NULL THEN 'appearance'
                    WHEN value IS NULL AND prev_value IS NOT NULL THEN 'disappearance'
                    ELSE 'change'
                END AS event_type,
                LEAD(CASE
                    WHEN prev_value IS NULL AND value IS NOT NULL THEN 'appearance'
                    WHEN value IS NULL AND prev_value IS NOT NULL THEN 'disappearance'
                    ELSE 'change'
                END) OVER w AS next_type,
                LEAD(measurement_date) OVER w AS next_date
            FROM read_parquet('{bucket_glob}')
            WINDOW w AS (PARTITION BY domain, query_type ORDER BY measurement_date)
        """)

        # Event type counts
        for et, n in con.execute(
            "SELECT event_type, COUNT(*) FROM classified GROUP BY event_type"
        ).fetchall():
            counts[et] += n
            total_events += n

        # Unique domain counts
        ud, ur = con.execute(
            "SELECT COUNT(DISTINCT domain), COUNT(DISTINCT registrable_domain) FROM classified"
        ).fetchone()
        unique_domains_total += ud
        unique_reg_total += ur

        # Per query-type breakdown
        for qt, ud, ur, ap, dp, ch in con.execute("""
            SELECT
                query_type,
                COUNT(DISTINCT domain),
                COUNT(DISTINCT registrable_domain),
                COUNT(*) FILTER (WHERE event_type='appearance'),
                COUNT(*) FILTER (WHERE event_type='disappearance'),
                COUNT(*) FILTER (WHERE event_type='change')
            FROM classified
            GROUP BY query_type
        """).fetchall():
            qt_unique_domains[qt] += ud
            qt_unique_reg[qt] += ur
            qt_event_counts[qt]['appearance'] += ap
            qt_event_counts[qt]['disappearance'] += dp
            qt_event_counts[qt]['change'] += ch

        # Per-day event counts
        for d, n in con.execute(
            "SELECT measurement_date, COUNT(*) FROM classified GROUP BY measurement_date"
        ).fetchall():
            date_event_counts[d] += n
            n_days_set.add(d)
            if first_date is None or d < first_date:
                first_date = d
            if last_date is None or d > last_date:
                last_date = d

        # Churn: gap histogram (disappear followed by reappear)
        for gap, n in con.execute("""
            SELECT (next_date - measurement_date)::INTEGER, COUNT(*)
            FROM classified
            WHERE event_type = 'disappearance' AND next_type = 'appearance'
            GROUP BY 1
        """).fetchall():
            gap_map[gap] += n

        # Ephemeral pairs (appear → disappear)
        tp, ep, eu = con.execute("""
            SELECT
                COUNT(*) FILTER (WHERE event_type='appearance' AND next_type='disappearance'),
                COUNT(*) FILTER (WHERE event_type='appearance' AND next_type='disappearance'
                                  AND (next_date - measurement_date) < 7),
                COUNT(DISTINCT domain || '|' || query_type)
                    FILTER (WHERE event_type='appearance' AND next_type='disappearance'
                             AND (next_date - measurement_date) < 7)
            FROM classified
        """).fetchone()
        total_pairs += tp
        ephemeral_pairs += ep
        ephemeral_unique += eu

        # Span distribution (appear → disappear, first 30 days)
        for span, n in con.execute("""
            SELECT (next_date - measurement_date)::INTEGER, COUNT(*)
            FROM classified
            WHERE event_type = 'appearance' AND next_type = 'disappearance'
              AND (next_date - measurement_date) <= 30
            GROUP BY 1
        """).fetchall():
            span_map[span] += n

        # Multi-churn distribution
        for nd, dom in con.execute("""
            SELECT n_disap, COUNT(*)
            FROM (
                SELECT domain, query_type, COUNT(*) AS n_disap
                FROM classified
                WHERE event_type = 'disappearance'
                GROUP BY domain, query_type
            ) t
            GROUP BY n_disap
        """).fetchall():
            churn_distribution[nd] += dom

        # Top-volatile: keep running top-1000 via min-heap
        for domain, qt, tot, ch, dp, ap in con.execute("""
            SELECT
                domain, query_type,
                COUNT(*) AS total_events,
                COUNT(*) FILTER (WHERE event_type='change'),
                COUNT(*) FILTER (WHERE event_type='disappearance'),
                COUNT(*) FILTER (WHERE event_type='appearance')
            FROM classified
            GROUP BY domain, query_type
            ORDER BY total_events DESC
            LIMIT 1000
        """).fetchall():
            entry = (tot, domain, qt, ch, dp, ap)
            if len(volatile_heap) < 1000:
                heapq.heappush(volatile_heap, entry)
            elif tot > volatile_heap[0][0]:
                heapq.heapreplace(volatile_heap, entry)

        con.close()

    print(f"\n  {n_buckets_processed} buckets processed in {time.time() - t0:.1f}s", flush=True)

    # ---------- Derived values ----------
    n_appearances    = counts.get("appearance", 0)
    n_disappearances = counts.get("disappearance", 0)
    n_changes        = counts.get("change", 0)
    n_days           = len(n_days_set)

    # Grace-period suppression curve
    vals: list[int] = [0] * 31
    for d in range(1, 31):
        vals[d] = vals[d - 1] + gap_map.get(d, 0)
    marginal   = [vals[d] - vals[d - 1] for d in range(1, 31)]
    second_diff = [marginal[d] - marginal[d - 1] for d in range(1, 30)]
    knee_idx   = second_diff.index(min(second_diff))
    knee_day   = knee_idx + 2

    within_3d = sum(gap_map.get(d, 0) for d in range(1, 4))
    churns_with_reappear = sum(gap_map.values())

    # ---------- Report ----------

    section("1. Date range")
    emit(f"  First date    : {first_date}")
    emit(f"  Last date     : {last_date}")
    emit(f"  Days with data: {n_days}")

    section("2. Event type breakdown")
    emit(f"  Total events: {total_events:,}")
    emit("")
    emit(f"  {'Type':<15} {'Count':>12}  {'%':>7}")
    emit(f"  {'-'*15} {'-'*12}  {'-'*7}")
    for event_type, n in sorted(counts.items(), key=lambda x: -x[1]):
        emit(f"  {event_type:<15} {n:>12,}  {pct(n, total_events):>7}")
    emit("")
    emit(f"  Signal events (genuine changes): {n_changes:,}  ({pct(n_changes, total_events)})")
    emit(f"  Noise events (appear+disappear): {n_appearances + n_disappearances:,}  ({pct(n_appearances + n_disappearances, total_events)})")

    section("3. Unique domains")
    emit(f"  {'Query type':<10} {'Unique domains':>16}  {'Unique reg-domains':>20}")
    emit(f"  {'-'*10} {'-'*16}  {'-'*20}")
    for qt in sorted(qt_unique_domains):
        emit(f"  {qt:<10} {qt_unique_domains[qt]:>16,}  {qt_unique_reg[qt]:>20,}")
    emit(f"  {'(all)':<10} {unique_domains_total:>16,}  {unique_reg_total:>20,}")
    emit("  (Domains with both MX and TXT events counted once per type)")

    section("4. Churn — disappear and reappear within N days")
    emit(f"  Total disappearances         : {n_disappearances:,}")
    emit(f"  Followed by re-appearance    : {churns_with_reappear:,}  ({pct(churns_with_reappear, n_disappearances)})")
    emit(f"  Never reappeared (or pending): {n_disappearances - churns_with_reappear:,}  ({pct(n_disappearances - churns_with_reappear, n_disappearances)})")
    emit("")
    emit("  Cumulative reappearance within N days (of all disappearances):")
    emit(f"  {'Days':>5}  {'Cum. reappear':>14}  {'% of disappear':>16}  {'% of disappear+reappear':>24}")
    emit(f"  {'-----':>5}  {'-------------':>14}  {'---------------':>16}  {'------------------------':>24}")
    for g in GRACE_CANDIDATES:
        cumulative = sum(gap_map.get(d, 0) for d in range(1, g + 1))
        emit(
            f"  {g:>5}  {cumulative:>14,}  {pct(cumulative, n_disappearances):>16}"
            f"  {pct(cumulative, churns_with_reappear):>24}"
        )
    emit("")
    emit(f"  >> 3-day churn (disappear+reappear ≤3 days): {within_3d:,}  ({pct(within_3d, n_disappearances)} of disappearances)")

    section("5. Optimal grace period")
    emit("  Suppression curve (grace period G suppresses disappearances")
    emit("  where the domain reappeared within G days, hiding churn noise):")
    emit("")
    emit(f"  {'Grace (days)':>12}  {'Suppressed events':>18}  {'% of disappear':>16}  {'Marginal gain/day':>18}")
    emit(f"  {'------------':>12}  {'------------------':>18}  {'----------------':>16}  {'------------------':>18}")
    for g in GRACE_CANDIDATES:
        sup = vals[min(g, 30)]
        mg  = marginal[g - 1] if g <= 30 else 0
        emit(f"  {g:>12}  {sup:>18,}  {pct(sup, n_disappearances):>16}  {mg:>18,}")
    emit("")
    emit(f"  Elbow point (knee of suppression curve): ~{knee_day} days")
    emit(f"  Recommendation: GRACE_PERIOD_DAYS = {knee_day}")
    sup_at_knee = vals[min(knee_day, 30)]
    emit(f"  At this grace period: {sup_at_knee:,} churn events suppressed")
    emit(f"  ({pct(sup_at_knee, n_disappearances)} of all disappearances treated as noise)")

    section("6. Ephemeral domains (active < 7 days)")
    emit(f"  Appearance→disappearance pairs total : {total_pairs:,}")
    emit(f"  Pairs with active span < 7 days       : {ephemeral_pairs:,}  ({pct(ephemeral_pairs, total_pairs)} of ap→dp pairs)")
    emit(f"  Unique (domain, query_type) ephemeral : {ephemeral_unique:,}")
    emit("")
    emit("  Span distribution for appearance→disappearance pairs:")
    emit(f"  {'Span (days)':>11}  {'Count':>10}  {'Cum. %':>8}")
    emit(f"  {'-----------':>11}  {'------':>10}  {'------':>8}")
    cumspan = 0
    for span in sorted(span_map)[:31]:
        n = span_map[span]
        cumspan += n
        marker = "  << 1 week" if span == 6 else ""
        emit(f"  {span:>11}  {n:>10,}  {pct(cumspan, total_pairs):>8}{marker}")

    section("7. MX vs TXT split")
    emit(f"  {'Type':<6} {'Total':>10}  {'Appear':>10}  {'Disappear':>10}  {'Change':>10}  {'Unique dom':>12}")
    emit(f"  {'------':<6} {'-----':>10}  {'------':>10}  {'---------':>10}  {'------':>10}  {'----------':>12}")
    for qt in sorted(qt_event_counts):
        ap  = qt_event_counts[qt]['appearance']
        dp  = qt_event_counts[qt]['disappearance']
        ch  = qt_event_counts[qt]['change']
        tot = ap + dp + ch
        ud  = qt_unique_domains[qt]
        emit(f"  {qt:<6} {tot:>10,}  {ap:>10,}  {dp:>10,}  {ch:>10,}  {ud:>12,}")

    section("8. Events per day distribution")
    if date_event_counts:
        day_counts   = list(date_event_counts.values())
        min_d        = min(day_counts)
        max_d        = max(day_counts)
        avg_d        = sum(day_counts) / len(day_counts)
        sorted_dc    = sorted(day_counts)
        mid          = len(sorted_dc) // 2
        median_d     = (sorted_dc[mid] + sorted_dc[~mid]) / 2
        emit(f"  Min events/day    : {int(min_d):,}")
        emit(f"  Max events/day    : {int(max_d):,}")
        emit(f"  Avg events/day    : {int(avg_d):,}")
        emit(f"  Median events/day : {int(median_d):,}")

        low_rows = sorted(
            [(d, n) for d, n in date_event_counts.items() if n < 100],
            key=lambda x: x[0],
        )
        if low_rows:
            emit(f"\n  Days with < 100 events (possible gaps): {len(low_rows)}")
            for d, n in low_rows[:20]:
                emit(f"    {d}: {n} events")
            if len(low_rows) > 20:
                emit(f"    ... and {len(low_rows) - 20} more")
        else:
            emit("\n  No days with < 100 events (no obvious gaps)")

    section("9. Multi-churn domains")
    total_with_disap = sum(churn_distribution.values())
    multi_churn      = sum(n for nd, n in churn_distribution.items() if nd >= 2)
    emit(f"  Domains (domain, query_type) with ≥1 disappearance : {total_with_disap:,}")
    emit(f"  Domains with ≥2 disappearances (multi-churn)        : {multi_churn:,}  ({pct(multi_churn, total_with_disap)})")
    emit("")
    emit(f"  {'# disappearances':>16}  {'# domain,qt pairs':>18}")
    emit(f"  {'----------------':>16}  {'------------------':>18}")
    for nd in sorted(churn_distribution)[:15]:
        emit(f"  {nd:>16}  {churn_distribution[nd]:>18,}")

    section("10. Top 1000 most volatile domains (by total event count)")
    top_volatile = sorted(volatile_heap, key=lambda x: -x[0])
    emit(f"  {'Domain':<50} {'QT':<5} {'Total':>7}  {'Change':>7}  {'Disap':>7}  {'Appear':>7}")
    emit(f"  {'-'*50} {'--':<5} {'-----':>7}  {'------':>7}  {'-----':>7}  {'------':>7}")
    for tot, domain, qt, ch, dp, ap in top_volatile:
        d_disp = domain if len(domain) <= 50 else domain[:47] + "..."
        emit(f"  {d_disp:<50} {qt:<5} {tot:>7,}  {ch:>7,}  {dp:>7,}  {ap:>7,}")

    elapsed = time.time() - t0
    section(f"Summary (completed in {elapsed:.1f}s)")
    emit(f"  Date range        : {first_date} → {last_date} ({n_days} days)")
    emit(f"  Total events      : {total_events:,}")
    emit(f"  Unique domains    : {unique_domains_total:,}")
    emit(f"  Appearances       : {n_appearances:,}  ({pct(n_appearances, total_events)})")
    emit(f"  Disappearances    : {n_disappearances:,}  ({pct(n_disappearances, total_events)})")
    emit(f"  Genuine changes   : {n_changes:,}  ({pct(n_changes, total_events)})")
    emit(f"  3-day churn pairs : {within_3d:,}  ({pct(within_3d, n_disappearances)} of disappearances)")
    emit(f"  Optimal grace     : ~{knee_day} days")
    emit("")

    source_label = Path(args.data_dir).name
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = Path(args.output) if args.output else (
        _REPO_ROOT / 'results' / 'dataset_metrics' / f'metrics_{source_label}_{ts}.txt'
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out_lines))
    print(f"\nReport written to {out_path}")


if __name__ == "__main__":
    main()
