#!/usr/bin/env python3
"""
preprocess_parquet_v3.py — OpenINTEL email-record ingest pipeline
                           (per-bucket diff edition — large-scale memory optimised)


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MEMORY DESIGN  
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

At 200 M-domain zone-file scale, state alone is ~35 GB in RAM.  Three
additional allocations of similar magnitude would otherwise land in memory
simultaneously during the state write (Phase 7), pushing peak RSS above
140 GB.  The design choices below each prevent one of those spikes.

Explicit del of today_by_bucket after the diff loop
  today_by_bucket is built by _partition_by_bucket via pa.Table.take(),
  which copies rows into new Arrow buffers (~35 GB at 200 M-domain scale).
  The diff loop (Phase 5) is its last consumer; Phases 6 and 7 do not use
  it.  Python does not GC local variables mid-function, so without an
  explicit del the variable remains live through the entire state write.
  One del statement after the diff loop eliminates that cost before the
  Phase 7 peak.

Streaming state write via ParquetWriter
  Writing state by concatenating all 256 bucket slices into a single Arrow
  table before calling write_table is an O(state) allocation: at 35 GB of
  state, it doubles the state footprint momentarily.  Instead, a
  pq.ParquetWriter is opened once and each bucket slice is written
  individually; the writer holds only one row-group buffer (~tens of MB) at
  a time.  The resulting Parquet file is identical in schema, row-group size,
  and compression.

Chunked reg-domain computation via _compute_reg_and_bucket
  Computing registrable_domain and bucket requires calling tldextract for
  every unique query_name.  Materialising the full query_name column as a
  Python list and accumulating results in a single cross-run dict both scale
  linearly with N: at 200 M rows these objects together consume ~35 GB of
  Python heap and persist as locals in process_day through the diff loop and
  state write.  Processing query_name in CHUNK_SIZE slices instead bounds
  the Python allocation to O(CHUNK_SIZE) per iteration — ~150 MB at the
  default of 1 M rows.  Results are accumulated as compact Arrow arrays
  (~2 GB total at 200 M rows) and attached via append_column, which is
  zero-copy for the existing columns.
  Tradeoff: without a cross-chunk cache, domains whose MX and TXT rows fall
  in different chunks are processed by tldextract twice.  At 200 M rows with
  1 M chunks this costs ~50–70 M extra tldextract calls (~1–2 extra minutes),
  in exchange for eliminating ~35 GB of Python heap.

Peak RAM at 200 M domains (~35 GB state, S):

  Phase          without mitigations    with mitigations
  ───────────────────────────────────────────────────────
  3 reg-domain        ~122 GB               ~89 GB
  7 state write       ~142 GB               ~38 GB

Feasible on a 96–128 GB cloud instance.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CORE DESIGN: PER-BUCKET DIFF
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Runs 256 small joins, one per bucket. Each join sees at most ~1/256 of the
total data (~200 k rows at 50 M scale). The per-join hash table is ~20 MB
regardless of how many zone files are added. Total memory is bounded by design.

What this requires:

  1. Registrable-domain and bucket computed during AGGREGATION (before the
     diff), not only at enrichment time. tldextract is called once per
     unique query_name, using a Python dict so MX and TXT for the same
     domain share one PSL lookup.

  2. today_agg pre-partitioned into 256 Arrow slices in one O(N) pass
     (_partition_by_bucket). Each slice is registered into state_con just
     for its bucket's join, then immediately unregistered — no big table
     sits in state_con's memory for longer than one join.

  3. State is read into Arrow once at the start of each day and partitioned
     by bucket in Python (same O(N) pass). Both sides of each per-bucket
     join are pre-partitioned Arrow slices, so DuckDB never scans all of
     state inside the loop.

  4. Crash safety is preserved by a two-pass structure: collect ALL events
     across all 256 bucket iterations first, write the delta Parquet file
     second, upsert state last. A crash anywhere before the log is written
     means the day is re-run in full; duplicate delta rows produced by the
     re-run are deduplicated by compaction (SELECT DISTINCT *).

  5. State upserts use targeted DELETE by primary key, per-bucket, so each
     touches only ~N/256 state rows.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STATE TABLE SCHEMA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  domain             VARCHAR   PK component
  query_type         VARCHAR   PK component
  value              VARCHAR[] last known value; NULL if in grace period
  missing_since      DATE      non-NULL only while domain is in grace period
  registrable_domain VARCHAR   cached eTLD+1 — avoids recomputing tldextract
                               for domains that disappear (state-only row)
  bucket             INTEGER   enables per-bucket join filtering

Only live domains are kept. A row is deleted when a domain disappears
(GRACE=0) or when its grace period expires. This keeps state small and
prevents unbounded growth over a multi-year backfill.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EVENT STORE LAYOUT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

data/
  events/
    bucket=0/
      base.parquet                    — compacted bulk history (sorted, deduped)
      delta-YYYYMMDD-{uuid}.parquet   — one file per day, per touched bucket
    ...
    bucket=255/
  ingest_log.json                     — {"last_date": "YYYY-MM-DD"} — cheap resume

Bucket assignment: blake2b(registrable_domain.lower(), digest_size=4) % 256
All subdomains of a registrable domain (utwente.nl, mail.utwente.nl, …)
land in the same bucket, enabling both point and suffix queries.

Event schema (each row is a CHANGE, never a snapshot):

  domain            VARCHAR     queried hostname, e.g. mail.utwente.nl
  registrable_domain VARCHAR    eTLD+1
  domain_reversed   VARCHAR     nl.utwente.mail — enables prefix-range queries
  query_type        VARCHAR     'MX' or 'TXT'
  measurement_date  DATE        date the change was observed
  value             VARCHAR[]   new value; NULL means domain disappeared
  prev_value        VARCHAR[]   previous value; NULL means first appearance
  bucket            INTEGER     redundant with path — helps DuckDB pushdown

No row has both value=NULL and prev_value=NULL. That would mean a domain
appeared and disappeared in the same snapshot, which cannot happen.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CASE CLASSIFICATION  (per domain, per query_type, per day)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  new_domain           First time seen. Event: NULL → value.
  genuine_change       Value changed while present. Event: old → new.
  new_disappearance    Absent today, was present.
                         GRACE=0: event (value → NULL), delete state row.
                         GRACE>0: set missing_since, no event yet.
  in_grace             Still absent, within grace window. No action.
  grace_expired        Absent for > GRACE_PERIOD_DAYS days.
                         Event dated to missing_since (value → NULL).
  churn                Reappeared within grace with same value.
                         Reset missing_since to NULL. No event — this was
                         just toplist noise, not a real change.
  genuine_reappearance Reappeared after grace, or with different value.
                         TWO events: disappearance at missing_since,
                         then reappearance at today.

in_grace and churn never produce events. no_change rows are excluded
by the WHERE clause before the actionable table is materialised.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PER-DAY COMMIT ORDER  (crash safety)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. Diff all buckets, collect actionable rows and build event lists
  2. Write delta Parquet files     ← crash here → duplicate events on re-run;
                                     compaction deduplicates via DISTINCT *
  3. Upsert state for all buckets  ← crash here → state is ahead; next run
                                     re-processes the day, finds no diff,
                                     writes nothing, advances the log
  4. Write ingest_log.json (atomic .tmp + os.replace)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXPECTED PERFORMANCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Top-lists only (~3–5 M domains at steady state):
    Fetch         ~2–3s    parallel S3 reads, 5 sources
    Aggregation   ~1–2s    DISTINCT + GROUP BY in DuckDB
    Reg-domain    ~4–6s    tldextract on ~3 M unique domains
    Diff          ~5–10s   256 tiny bucket joins
    Delta write   ~1–2s    COPY TO PARQUET with PARTITION_BY
    State upsert  ~5–10s   256 targeted DELETE + INSERT ON CONFLICT
    Target: ~20–35s per day.

  With zone files added (~50 M domains total):
    Reg-domain    ~30–60s  scales linearly with unique domain count
    Diff          constant  ~5–10s — each bucket still sees N/256 rows
    State upsert  constant  ~5–10s — same reasoning
    Zone files add ~30–60s/day, mostly tldextract. The diff and upsert
    do NOT grow with dataset size — that is the whole point of this design.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RUNNING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  python preprocess_parquet_v3.py                     # ingest (auto-resumes)
  python preprocess_parquet_v3.py --compact           # compact over threshold
  python preprocess_parquet_v3.py --force-compact     # compact everything
  python preprocess_parquet_v3.py --force-compact --workers 8
"""

import argparse
import datetime
import hashlib
import json
import os
import pyarrow.compute as pc
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import tldextract as _tldextract_lib

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
_env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip())

# TEMP: openintel-public uses anonymous access — credentials not needed
# KEY_ID = os.environ['OPENINTEL_KEY_ID']
# SECRET  = os.environ['OPENINTEL_SECRET']
KEY_ID = ''
SECRET  = ''

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_ENDPOINT = 'storage.dacs.utwente.nl'
# TEMP: switched to public bucket (anonymous access, internal endpoint)
# S3_BUCKET   = 'openintel'
S3_BUCKET   = 'openintel-public'

TOPLIST_SOURCES  = [] #["umbrella", "tranco", "radar", "alexa", "majestic"]
ZONEFILE_SOURCES = ["ee", "fr", "gov", "li", "nu", "se", "sk"]

SOURCES = (
    [("toplist",  src) for src in TOPLIST_SOURCES] +
    [("zonefile", src) for src in ZONEFILE_SOURCES]
)

start_date = datetime.date(2016, 1, 22)
end_date   = datetime.date(2026, 1,  1)

_REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
DATA_DIR   = 'data/zonefiles'   # relative to repo root — change to use a different dataset directory
BASE_DIR   = os.path.join(_REPO_ROOT, DATA_DIR)
EVENTS_DIR = os.path.join(BASE_DIR, 'events')
STATE_PARQUET = os.path.join(BASE_DIR, 'state.parquet')
INGEST_LOG    = os.path.join(BASE_DIR, 'ingest_log.json')

MAIN_CON_MEMORY  = '24GB'
MAIN_CON_THREADS = 6
STATE_CON_MEMORY = '8GB'

COMPACTION_DELTA_BYTES       = 500 * 1024 * 1024  # inline compaction trigger
FINAL_COMPACTION_DELTA_BYTES =  10 * 1024 * 1024  # post-backfill cleanup threshold
MAX_COMPACTIONS_PER_DAY = 16  # cap inline compactions to avoid stalling ingest

# Set to 0 to record every disappearance immediately.
# Set to N>0 to suppress domains that vanish and reappear within N days
# with the same value (toplist churn noise).
GRACE_PERIOD_DAYS = 0

# Each chunk materialises ~CHUNK_SIZE Python strings (~77 MB at 1 M) plus a
# per-chunk dedup cache (~75 MB at 500 K unique entries) before both are freed.
# Reducing this trades slightly more tldextract work for lower per-chunk RAM.
CHUNK_SIZE = 1_000_000

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
_local       = threading.local()   # per-thread S3 connections
_insert_lock = threading.Lock()    # serialise Arrow→DuckDB loads in fetch phase

# One TLDExtract instance shared across threads; PSL is loaded once at startup.
_tld = _tldextract_lib.TLDExtract()

# ---------------------------------------------------------------------------
# Arrow type policy: int32-offset overflow at scale
#
# Arrow's `string` and `list<...>` types use int32 offsets, capping a single
# contiguous array at ~2 GB of string bytes / 2**31 list child elements. At
# zone-file scale a fresh-start day buffers ~all 200 M domains into one
# events_enriched table, blowing past that cap ("a single pyarrow column
# exceeds the 32-bit pointer size"). large_string / large_list use int64
# offsets and remove the cap.
#
# All in-memory STATE and EVENT tables therefore use the LARGE_* types below.
# DuckDB reads these fine and still writes plain VARCHAR/LIST Parquet, so the
# on-disk logical schema (and every downstream reader) is unchanged. The one
# remaining ceiling is the int32 *length* field: a single Arrow table is capped
# at 2**31-1 rows regardless of offset width — far above a worst-case day's
# ~400 M event rows at 200 M-domain scale. Beyond that, the day's events would
# need to be emitted in row-batches.
#
# DuckDB->Arrow conversions always emit the narrow int32 types, so any
# DuckDB-produced table that feeds the in-memory state must be passed through
# _cast_to_schema first (see _load_state, _rebuild_state_from_events).
# ---------------------------------------------------------------------------
LARGE_STR  = pa.large_string()
LARGE_LIST = pa.large_list(pa.large_string())

# Sentinel empty tables used when a bucket has no rows on one side of the join.
# These also define the canonical wide schema for state slices and the
# _save_state ParquetWriter.
_EMPTY_BUCKET = pa.table({
    'query_name':          pa.array([], type=LARGE_STR),
    'query_type':          pa.array([], type=LARGE_STR),
    'value':               pa.array([], type=LARGE_LIST),
    'registrable_domain':  pa.array([], type=LARGE_STR),
    'bucket':              pa.array([], type=pa.int32()),
})
_EMPTY_STATE = pa.table({
    'domain':              pa.array([], type=LARGE_STR),
    'query_type':          pa.array([], type=LARGE_STR),
    'value':               pa.array([], type=LARGE_LIST),
    'missing_since':       pa.array([], type=pa.date32()),
    'registrable_domain':  pa.array([], type=LARGE_STR),
    'bucket':              pa.array([], type=pa.int32()),
})
_STATE_SCHEMA = _EMPTY_STATE.schema


def _cast_to_schema(tbl: pa.Table, schema: pa.Schema) -> pa.Table:
    """Cast a (possibly DuckDB-produced, int32-typed) table to a wide canonical
    schema. No-op when already matching. Used at every DuckDB->Arrow and
    Parquet-read boundary that feeds the in-memory state, so all state slices
    are uniformly large_* and concat/filter never hit a type mismatch."""
    return tbl if tbl.schema.equals(schema) else tbl.cast(schema)
# ---------------------------------------------------------------------------
# Domain helpers
# ---------------------------------------------------------------------------

def _reg_domain(domain: str) -> str:
    """eTLD+1 via the Public Suffix List. Falls back to the domain itself."""
    if not domain:
        return domain
    rd = _tld(domain.lower()).top_domain_under_public_suffix
    return rd if rd else domain.lower()


def _rev_domain(domain: str) -> str:
    """Label-reverse: mail.utwente.nl → nl.utwente.mail.
    Stored in events to enable efficient prefix-range queries in DuckDB
    (WHERE domain_reversed LIKE 'nl.utwente.%')."""
    if not domain:
        return domain
    return '.'.join(reversed(domain.lower().split('.')))


def _bucket(reg_dom: str) -> int:
    """Deterministic bucket 0–255. blake2b, not Python's hash() (randomised per process)."""
    key = (reg_dom or '').lower().encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=4).digest(), 'big') % 256


# ---------------------------------------------------------------------------
# S3 / fetch helpers
# ---------------------------------------------------------------------------

def _s3_path(basis: str, source: str, d: datetime.date) -> str:
    if basis == "toplist":
        # TEMP: toplist path unchanged — only zonefiles moved to openintel-public
        return (
            f"s3://{S3_BUCKET}/catalog/warehouse/fdns/data/"
            f"source={source}/date={d}/*.parquet"
        )
    # TEMP: new public bucket uses fdns/basis=zonefile/ path layout
    # Old: f"s3://{S3_BUCKET}/category=fdns/type=warehouse/source={source}/year={d.year}/month={d.month:02d}/day={d.day:02d}/*.parquet"
    return (
        f"s3://{S3_BUCKET}/fdns/basis=zonefile/"
        f"source={source}/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/*.parquet"
    )


def _secret_sql() -> str:
    return (
        f"CREATE OR REPLACE SECRET openintel ("
        f"TYPE S3, KEY_ID '{KEY_ID}', SECRET '{SECRET}', "
        f"REGION 'us-east-1', ENDPOINT '{S3_ENDPOINT}', "
        f"URL_STYLE 'path', USE_SSL true);"
    )


def _fetch_con() -> duckdb.DuckDBPyConnection:
    """One in-memory DuckDB connection per thread, with S3 credentials loaded."""
    if not hasattr(_local, 'con'):
        c = duckdb.connect()
        c.execute("INSTALL aws; LOAD aws;")
        c.execute(_secret_sql())
        c.execute("PRAGMA threads=1;")
        _local.con = c
    return _local.con


def fetch_source(basis: str, source: str, d: datetime.date):
    """Fetch one source for one day from S3. Returns an Arrow table or None."""
    path = _s3_path(basis, source, d)
    try:
        result = _fetch_con().execute(f"""
            SELECT query_name, query_type, COALESCE(mx_address, txt_text) AS val
            FROM read_parquet('{path}')
            WHERE (query_type = 'MX'  AND mx_address IS NOT NULL)
               OR (query_type = 'TXT' AND txt_text   IS NOT NULL)
        """).to_arrow_table()
        print(f"  ✓ {basis}/{source}: {result.num_rows:,} rows")
        return result
    except Exception as e:
        if "No files found" in str(e) or "doesn't exist" in str(e):
            print(f"  - {basis}/{source}: no data")
        else:
            print(f"  ! {basis}/{source}: {e}")
        return None


# ---------------------------------------------------------------------------
# Connection setup
# ---------------------------------------------------------------------------

def _main_con() -> duckdb.DuckDBPyConnection:
    """In-memory connection used for S3 fetching and delta Parquet writes."""
    c = duckdb.connect()
    c.execute(f"PRAGMA threads={MAIN_CON_THREADS};")
    c.execute(f"PRAGMA memory_limit='{MAIN_CON_MEMORY}';")
    c.execute("PRAGMA preserve_insertion_order=false;")
    c.execute("SET http_keep_alive=true;")
    c.execute("INSTALL aws; LOAD aws;")
    c.execute(_secret_sql())
    return c


def _query_con() -> duckdb.DuckDBPyConnection:
    """Pure in-memory DuckDB connection used only for per-bucket diff queries."""
    c = duckdb.connect()
    c.execute(f"PRAGMA memory_limit='{STATE_CON_MEMORY}';")
    c.execute(f"PRAGMA threads={MAIN_CON_THREADS};")
    return c


def _load_state() -> dict[int, pa.Table]:
    """Load state.parquet into a per-bucket Arrow dict."""
    os.makedirs(BASE_DIR, exist_ok=True)
    if os.path.exists(STATE_PARQUET):
        t0 = time.time()
        snap = pq.read_table(STATE_PARQUET)
        snap = _cast_to_schema(snap, _STATE_SCHEMA)
        result = (
            {k: v for k, v in _partition_by_bucket(snap).items() if k is not None}
            if snap.num_rows > 0 else {}
        )
        n = sum(t.num_rows for t in result.values())
        print(f"State loaded: {n:,} rows in {time.time() - t0:.1f}s")
        return result

    print("State loaded: 0 rows (fresh start)")
    return {}


def _save_state(state_by_bucket: dict[int, pa.Table]) -> None:
    """
    Atomically write the full in-memory state to state.parquet.

    Streams bucket slices into a ParquetWriter one at a time so peak
    additional allocation is a single row-group write buffer (~tens of MB).
    Concatenating all 256 slices into one Arrow table before writing would
    be an O(state) allocation — ~35 GB at 200 M-domain scale — on top of
    the already-live state_by_bucket.

    Crash safety is preserved: the writer targets a .tmp path; os.replace
    is called only after the writer closes (i.e. the file is fully flushed).
    A crash between the last bucket write and os.replace leaves the old
    state.parquet intact.
    """
    tmp = STATE_PARQUET + '.tmp'
    schema = _EMPTY_STATE.schema
    with pq.ParquetWriter(tmp, schema, compression='zstd') as writer:
        for tbl in state_by_bucket.values():
            if tbl.num_rows > 0:
                writer.write_table(tbl, row_group_size=100_000)
    # If state_by_bucket is empty, write_table is never called; ParquetWriter
    # still produces a valid empty Parquet file when the context exits.
    os.replace(tmp, STATE_PARQUET)


# ---------------------------------------------------------------------------
# Ingest log  (cheap resume without scanning Parquet)
# ---------------------------------------------------------------------------

def _load_last_date() -> datetime.date | None:
    try:
        with open(INGEST_LOG) as f:
            v = json.load(f).get('last_date')
            return datetime.date.fromisoformat(v) if v else None
    except Exception:
        return None


def _save_last_date(d: datetime.date) -> None:
    tmp = INGEST_LOG + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({'last_date': d.isoformat()}, f)
    os.replace(tmp, INGEST_LOG)


# ---------------------------------------------------------------------------
# Bootstrap: rebuild state from event store when state.parquet is missing
# ---------------------------------------------------------------------------

def _has_event_store() -> bool:
    if not os.path.isdir(EVENTS_DIR):
        return False
    for _, _, files in os.walk(EVENTS_DIR):
        if any(f.endswith('.parquet') for f in files):
            return True
    return False


def _rebuild_state_from_events(
    con: duckdb.DuckDBPyConnection,
) -> tuple[dict[int, pa.Table], datetime.date | None]:
    state: dict[int, pa.Table] = {}
    max_date: datetime.date | None = None

    bucket_dirs = sorted(
        (e for e in os.scandir(EVENTS_DIR) if e.is_dir()),
        key=lambda e: e.name,
    )
    for entry in bucket_dirs:
        files = [
            os.path.join(entry.path, f)
            for f in os.listdir(entry.path)
            if f.endswith('.parquet')
        ]
        if not files:
            continue

        bucket_id = int(entry.name.split('=')[1])
        files_sql = ', '.join(f"'{f}'" for f in files)

        tbl = con.execute(f"""
            SELECT
                domain, query_type,
                arg_max(value, measurement_date)                AS value,
                CASE WHEN arg_max(value, measurement_date) IS NULL
                     THEN MAX(measurement_date) ELSE NULL END   AS missing_since,
                ANY_VALUE(registrable_domain)                   AS registrable_domain,
                CAST(ANY_VALUE(bucket) AS INTEGER)              AS bucket,
                MAX(measurement_date)                           AS _max_date
            FROM read_parquet([{files_sql}])
            GROUP BY domain, query_type
        """).arrow().read_all()

        if tbl.num_rows:
            bucket_max = pc.max(tbl.column('_max_date')).as_py()
            tbl = tbl.drop(['_max_date'])
            tbl = _cast_to_schema(tbl, _STATE_SCHEMA)
            state[bucket_id] = tbl
            if max_date is None or bucket_max > max_date:
                max_date = bucket_max

    return state, max_date


def _bootstrap_if_needed(
    con: duckdb.DuckDBPyConnection,
) -> datetime.date | None:
    """
    If state.parquet is missing but the event store exists, rebuild state
    from events. Returns the derived last-processed date (max measurement_date
    across all events) so do_ingest() can seed the ingest log if it too is absent.

    Returns None if state already exists or there is no event store (fresh start).
    """
    if os.path.exists(STATE_PARQUET):
        # Also treat a zero-row state as absent — an empty state.parquet with an
        # existing event store means a corrupted or premature write; rebuilding
        # prevents the pipeline from registering every active domain as new_domain.
        try:
            if pq.read_metadata(STATE_PARQUET).num_rows > 0:
                return None
            print("state.parquet exists but is empty — treating as absent.")
        except Exception:
            return None  # unreadable state: let _load_state surface the error
    if not _has_event_store():
        return None

    print("state.parquet absent but event store found — rebuilding state from events...")
    t0 = time.time()
    state, last_date = _rebuild_state_from_events(con)
    n = sum(t.num_rows for t in state.values())
    _save_state(state)
    print(f"State rebuilt: {n:,} rows in {time.time() - t0:.1f}s  (last event date: {last_date})")
    return last_date


# ---------------------------------------------------------------------------
# Bucketing helpers
# ---------------------------------------------------------------------------

def _partition_by_bucket(table: pa.Table) -> dict[int, pa.Table]:
    """
    Split an Arrow table into per-bucket slices in one O(N) pass.

    Returns a dict {bucket_int: Arrow table}. Only buckets present in the
    table are included. Each entry is produced by pa.Table.take() (gather by
    index), which copies the selected rows into new buffers.
    """
    bucket_col = table.column('bucket').to_pylist()
    # Build per-bucket row-index lists in a single linear scan.
    indices: dict[int, list[int]] = {}
    for i, b in enumerate(bucket_col):
        if b not in indices:
            indices[b] = []
        indices[b].append(i)
    return {b: table.take(idxs) for b, idxs in indices.items()}


# ---------------------------------------------------------------------------
# Reg-domain computation — chunked, bounded-memory helper
# ---------------------------------------------------------------------------

def _compute_reg_and_bucket(today_agg: pa.Table) -> pa.Table:
    """
    Attach registrable_domain and bucket columns to today_agg.

    Processes query_name in CHUNK_SIZE slices to keep peak Python allocation
    at O(CHUNK_SIZE) regardless of total row count.

    The naive approach — materialising the full query_name column as a Python
    list and collecting results in a single cross-run dict — scales linearly
    with N.  At 200 M rows those two objects together consume ~32 GB of Python
    heap and persist as locals in process_day through the diff loop and state
    write.  Chunking avoids this: each iteration materialises ~77 MB of Python
    strings and a per-chunk dedup cache (~75 MB at 500 K unique entries), both
    freed before the next iteration.  Results accumulate as compact Arrow arrays
    (~1.9 GB total at 200 M rows).

    pa.Table.append_column is zero-copy for existing columns — only the two new
    column buffers (~1.9 GB) are freshly allocated.

    Tradeoff: without a cross-chunk cache, domains whose MX and TXT rows fall
    in different chunks are processed by tldextract twice.  Keeping a cross-chunk
    dict would reintroduce the O(N) memory cost.  At 200 M rows with CHUNK_SIZE=1M
    this costs ~50–70 M extra tldextract calls (~1–2 extra minutes).
    """
    n          = today_agg.num_rows
    qnames_col = today_agg.column('query_name')  # Arrow buffer reference, no copy
    reg_chunks: list[pa.Array] = []
    bkt_chunks: list[pa.Array] = []

    for start in range(0, n, CHUNK_SIZE):
        length     = min(CHUNK_SIZE, n - start)
        chunk_list = qnames_col.slice(start, length).to_pylist()  # ~77 MB at 1 M rows
        cache: dict[str, str] = {}
        reg_list: list[str] = []
        bkt_list: list[int] = []
        for d in chunk_list:
            rd = cache.get(d)
            if rd is None:
                rd       = _reg_domain(d)
                cache[d] = rd
            reg_list.append(rd)
            bkt_list.append(_bucket(rd))
        # Narrow `string` (int32 offsets) is safe here: CHUNK_SIZE bounds each
        # array to ~1M domain strings (~30 MB), far under the 2 GB cap. See the
        # LARGE_STR/LARGE_LIST note near the sentinel tables for the paths that
        # do need the wide types.
        reg_chunks.append(pa.array(reg_list, pa.string()))
        bkt_chunks.append(pa.array(bkt_list, pa.int32()))
        del chunk_list, cache, reg_list, bkt_list  # free before next iteration

    today_agg = today_agg.append_column(
        'registrable_domain', pa.chunked_array(reg_chunks)
    )
    today_agg = today_agg.append_column(
        'bucket', pa.chunked_array(bkt_chunks)
    )
    del reg_chunks, bkt_chunks
    return today_agg


# ---------------------------------------------------------------------------
# Core: process one day
# ---------------------------------------------------------------------------

def process_day(
    current_date: datetime.date,
    main_con: duckdb.DuckDBPyConnection,
    query_con: duckdb.DuckDBPyConnection,
    state_by_bucket: dict[int, pa.Table],
) -> tuple[int, set[int], dict[int, pa.Table]]:
    """
    Ingest one day. Returns (event_count, touched_buckets, updated_state_by_bucket).

    state_by_bucket is kept in memory across days — no DuckDB read on day 2+.

    Commit order (crash safety):
      1. Diff all buckets  →  collect events + actionable tables in memory
      2. Write delta Parquet files
      3. Update state_by_bucket in Python, persist to state.parquet
      4. Caller writes ingest_log.json
    """
    os.makedirs(EVENTS_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Fetch all sources in parallel into a temp table
    # ------------------------------------------------------------------
    t_fetch = time.time()
    main_con.execute("""
        CREATE OR REPLACE TEMP TABLE raw_today (
            query_name VARCHAR, query_type VARCHAR, val VARCHAR
        )
    """)
    with ThreadPoolExecutor(max_workers=max(len(SOURCES), 1)) as executor:
        futures = {
            executor.submit(fetch_source, basis, source, current_date): (basis, source)
            for basis, source in SOURCES
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                with _insert_lock:
                    main_con.register("_incoming", result)
                    main_con.execute("INSERT INTO raw_today SELECT * FROM _incoming")
                    main_con.unregister("_incoming")
                del result

    row_count = main_con.execute("SELECT COUNT(*) FROM raw_today").fetchone()[0]
    print(f"  Fetch: {time.time() - t_fetch:.1f}s — {row_count:,} rows")
    if row_count == 0:
        print(f"  No data for {current_date}, skipping.")
        return 0, set(), {}

    # ------------------------------------------------------------------
    # 2. Aggregate: one sorted, deduplicated value list per (domain, type)
    # ------------------------------------------------------------------
    t0 = time.time()
    today_agg = main_con.execute("""
        SELECT query_name, query_type, list_sort(list(val)) AS value
        FROM (SELECT DISTINCT query_name, query_type, val FROM raw_today)
        GROUP BY query_name, query_type
    """).to_arrow_table()
    print(f"  Aggregation: {time.time() - t0:.1f}s"
          f" — {today_agg.num_rows:,} unique (domain, type) pairs")

    # ------------------------------------------------------------------
    # 3. Compute registrable_domain + bucket for every unique domain.
    #
    # Delegated to _compute_reg_and_bucket, which processes query_name in
    # CHUNK_SIZE slices to bound peak Python allocation to O(CHUNK_SIZE).
    # See that function's docstring for the memory analysis and tradeoff.
    # ------------------------------------------------------------------
    t0 = time.time()
    today_agg = _compute_reg_and_bucket(today_agg)
    print(f"  Reg-domain: {time.time() - t0:.1f}s")

    # ------------------------------------------------------------------
    # 4. Pre-partition today_agg and state by bucket (one O(N) pass each).
    #
    # Both are read into Arrow and split into 256 slices before the diff
    # loop. This is critical: querying state inside the loop with
    # "WHERE bucket = X" causes DuckDB to do a full table scan of state
    # for EACH of the 256 iterations (256 × N rows), which at 3 M rows
    # takes ~500s. Reading state once and slicing in Python is O(N) total.
    # ------------------------------------------------------------------
    today_by_bucket = _partition_by_bucket(today_agg)
    del today_agg

    all_buckets = sorted(set(state_by_bucket.keys()) | set(today_by_bucket.keys()))

    # ------------------------------------------------------------------
    # 5. Per-bucket diff loop  (PASS 1 — collect; do NOT upsert state yet)
    #
    # For each bucket, both sides of the join are pre-partitioned Arrow
    # slices (~N/256 rows each). DuckDB joins two small in-memory tables
    # instead of scanning all of state once per bucket.
    # ------------------------------------------------------------------
    d_sql = f"CAST('{current_date}' AS DATE)"

    ev_domains, ev_qtypes, ev_values, ev_prev = [], [], [], []
    ev_dates, ev_reg_domains, ev_buckets = [], [], []
    counts: dict[str, int] = {}
    bucket_actionables: dict[int, pa.Table] = {}
    t_diff = time.time()

    for bucket in all_buckets:
        bucket_today = today_by_bucket.get(bucket, _EMPTY_BUCKET)
        bucket_state = state_by_bucket.get(bucket, _EMPTY_STATE)

        query_con.register('_bucket_today', bucket_today)
        query_con.register('_bucket_state', bucket_state)

        # Both sides are pre-partitioned Arrow tables (~N/256 rows each).
        # The WHERE clause mirrors the CASE so no_change and in_grace rows
        # are excluded before the result is materialised in Python.
        actionable = query_con.execute(f"""
            SELECT
                COALESCE(t.query_name, s.domain)                      AS domain,
                COALESCE(t.query_type, s.query_type)                  AS query_type,
                t.value                                               AS today_value,
                s.value                                               AS state_value,
                s.missing_since,
                COALESCE(t.registrable_domain, s.registrable_domain)  AS registrable_domain,
                CASE
                    WHEN t.query_name IS NULL THEN
                        CASE
                            WHEN s.missing_since IS NULL AND s.value IS NOT NULL
                                THEN 'new_disappearance'
                            WHEN s.missing_since IS NOT NULL
                                 AND ({d_sql} - s.missing_since) > {GRACE_PERIOD_DAYS}
                                THEN 'grace_expired'
                            ELSE 'in_grace'
                        END
                    WHEN s.missing_since IS NOT NULL THEN
                        CASE
                            WHEN t.value = s.value
                                 AND ({d_sql} - s.missing_since) <= {GRACE_PERIOD_DAYS}
                                THEN 'churn'
                            ELSE 'genuine_reappearance'
                        END
                    WHEN s.domain IS NULL                    THEN 'new_domain'
                    WHEN t.value IS DISTINCT FROM s.value    THEN 'genuine_change'
                    ELSE 'no_change'
                END AS case_type
            FROM _bucket_today t
            FULL OUTER JOIN _bucket_state s
                ON t.query_name = s.domain AND t.query_type = s.query_type
            WHERE CASE
                    WHEN t.query_name IS NULL THEN
                        CASE
                            WHEN s.missing_since IS NULL AND s.value IS NOT NULL
                                THEN TRUE
                            WHEN s.missing_since IS NOT NULL
                                 AND ({d_sql} - s.missing_since) > {GRACE_PERIOD_DAYS}
                                THEN TRUE
                            ELSE FALSE        -- in_grace: excluded
                        END
                    WHEN s.missing_since IS NOT NULL THEN TRUE   -- churn or genuine_reappearance
                    WHEN s.domain IS NULL            THEN TRUE   -- new_domain
                    WHEN t.value IS DISTINCT FROM s.value THEN TRUE
                    ELSE FALSE                                   -- no_change: excluded
                  END
        """).to_arrow_table()
        query_con.unregister('_bucket_today')
        query_con.unregister('_bucket_state')

        if actionable.num_rows == 0:
            continue

        bucket_actionables[bucket] = actionable

        # Build event rows from this bucket's actionable (one Python pass).
        for domain, qtype, today_val, state_val, missing_since, reg_dom, case_type in zip(
            actionable.column('domain').to_pylist(),
            actionable.column('query_type').to_pylist(),
            actionable.column('today_value').to_pylist(),
            actionable.column('state_value').to_pylist(),
            actionable.column('missing_since').to_pylist(),
            actionable.column('registrable_domain').to_pylist(),
            actionable.column('case_type').to_pylist(),
        ):
            counts[case_type] = counts.get(case_type, 0) + 1

            if case_type in ('new_domain', 'genuine_change'):
                ev_domains.append(domain);    ev_qtypes.append(qtype)
                ev_values.append(today_val);  ev_prev.append(state_val)
                ev_dates.append(current_date); ev_reg_domains.append(reg_dom)
                ev_buckets.append(bucket)

            elif case_type == 'new_disappearance' and GRACE_PERIOD_DAYS == 0:
                ev_domains.append(domain);   ev_qtypes.append(qtype)
                ev_values.append(None);      ev_prev.append(state_val)
                ev_dates.append(current_date); ev_reg_domains.append(reg_dom)
                ev_buckets.append(bucket)

            elif case_type == 'grace_expired':
                # Date the event to when the domain actually went missing,
                # not today — we only confirmed the disappearance today.
                ev_domains.append(domain);   ev_qtypes.append(qtype)
                ev_values.append(None);      ev_prev.append(state_val)
                ev_dates.append(missing_since); ev_reg_domains.append(reg_dom)
                ev_buckets.append(bucket)

            elif case_type == 'genuine_reappearance':
                # Two events: one for the disappearance (at missing_since),
                # one for the reappearance (today). This preserves the exact
                # timeline without needing to back-fill old delta files.
                ev_domains     += [domain, domain]
                ev_qtypes      += [qtype,  qtype]
                ev_values      += [None,   today_val]
                ev_prev        += [state_val, None]
                ev_dates       += [missing_since, current_date]
                ev_reg_domains += [reg_dom, reg_dom]
                ev_buckets     += [bucket, bucket]
            # churn and new_disappearance-with-grace: state update only, no event

    event_count = len(ev_domains)
    print(
        f"  Diff: {time.time() - t_diff:.1f}s — {event_count:,} events  "
        f"new={counts.get('new_domain', 0):,}  "
        f"changed={counts.get('genuine_change', 0):,}  "
        f"disappeared={counts.get('new_disappearance', 0):,}  "
        f"grace_expired={counts.get('grace_expired', 0):,}  "
        f"reappeared={counts.get('genuine_reappearance', 0):,}  "
        f"churn={counts.get('churn', 0):,}"
    )

    # today_by_bucket's last consumer was the diff loop above.  Phases 6
    # and 7 do not use it.  Without an explicit del, Python keeps the variable
    # alive until process_day returns, pinning ~today_agg worth of Arrow
    # buffers (~35 GB at 200 M-domain scale) through the entire _save_state
    # call.
    del today_by_bucket

    # ------------------------------------------------------------------
    # 6. Write delta Parquet files  (MUST happen before state upsert)
    #
    # domain_reversed is computed here (only for event rows, not all
    # domains) since it isn't needed for bucketing. reg_domain and bucket
    # are already known from the diff loop.
    # ------------------------------------------------------------------
    touched_buckets: set[int] = set()
    if event_count > 0:
        t0 = time.time()
        domain_reversed = [_rev_domain(d) for d in ev_domains]
        touched_buckets = set(ev_buckets)

        # large_string/large_list (int64 offsets): on a fresh-start/heavy-change
        # day at zone-file scale, nearly every domain lands here, so these
        # columns can exceed the 2 GB int32-offset cap. See the LARGE_STR /
        # LARGE_LIST comment near the sentinel tables above. DuckDB still
        # writes plain VARCHAR/LIST Parquet from this table.
        events_enriched = pa.table({
            'domain':             pa.array(ev_domains,       type=LARGE_STR),
            'registrable_domain': pa.array(ev_reg_domains,   type=LARGE_STR),
            'domain_reversed':    pa.array(domain_reversed,  type=LARGE_STR),
            'query_type':         pa.array(ev_qtypes,        type=LARGE_STR),
            'measurement_date':   pa.array(ev_dates,         type=pa.date32()),
            'value':              pa.array(ev_values,        type=LARGE_LIST),
            'prev_value':         pa.array(ev_prev,          type=LARGE_LIST),
            'bucket':             pa.array(ev_buckets,       type=pa.int32()),
        })

        date_str = current_date.strftime('%Y%m%d')
        main_con.register('_events_enriched', events_enriched)
        main_con.execute(f"""
            COPY (
                SELECT domain, registrable_domain, domain_reversed,
                       query_type, measurement_date, value, prev_value, bucket
                FROM _events_enriched
                ORDER BY bucket, registrable_domain, domain_reversed
            ) TO '{EVENTS_DIR}' (
                FORMAT PARQUET,
                CODEC 'ZSTD',
                PARTITION_BY (bucket),
                FILENAME_PATTERN 'delta-{date_str}-{{uuid}}',
                ROW_GROUP_SIZE 100000,
                OVERWRITE_OR_IGNORE TRUE
            )
        """)
        main_con.unregister('_events_enriched')
        del events_enriched
        print(f"  Delta write: {time.time() - t0:.1f}s")

    # ------------------------------------------------------------------
    # 7. Update in-memory state, then persist to state.parquet.
    #
    # In-memory: patch only changed buckets via _apply_actionable_to_state.
    # Persist: _save_state writes the full state as a single Parquet file
    # (atomic .tmp → os.replace).
    #
    # _save_state streams bucket slices via ParquetWriter, so no full-state
    # concat allocation is needed.  today_by_bucket was freed above.  Together
    # these keep Phase 7 peak at ~S (state size) rather than ~3S.
    # ------------------------------------------------------------------
    t0 = time.time()
    for bucket, actionable in bucket_actionables.items():
        old_slice = state_by_bucket.get(bucket, _EMPTY_STATE)
        new_slice = _apply_actionable_to_state(
            old_slice, actionable, current_date, bucket
        )
        if new_slice.num_rows > 0:
            state_by_bucket[bucket] = new_slice
        elif bucket in state_by_bucket:
            del state_by_bucket[bucket]

    _save_state(state_by_bucket)
    print(f"  State write: {time.time() - t0:.1f}s")

    return event_count, touched_buckets, state_by_bucket


def _apply_actionable_to_state(
    old_state: pa.Table,
    actionable: pa.Table,
    current_date: datetime.date,
    bucket: int,
) -> pa.Table:
    """
    Apply one bucket's actionable changes to its state slice. Pure Python + Arrow.

    Iterates over actionable rows only (small: ~k/256 per bucket).
    Uses pc.binary_join_element_wise + pc.is_in for a vectorized filter over
    old_state rows — no Python loop over state.
    """
    keys_to_remove: set[tuple] = set()
    new_domains, new_qtypes, new_values, new_missing, new_reg = [], [], [], [], []

    for domain, qtype, today_val, state_val, reg_dom, case_type in zip(
        actionable.column('domain').to_pylist(),
        actionable.column('query_type').to_pylist(),
        actionable.column('today_value').to_pylist(),
        actionable.column('state_value').to_pylist(),
        actionable.column('registrable_domain').to_pylist(),
        actionable.column('case_type').to_pylist(),
    ):
        keys_to_remove.add((domain, qtype))

        is_gone = (case_type == 'grace_expired' or
                   (case_type == 'new_disappearance' and GRACE_PERIOD_DAYS == 0))
        if is_gone:
            continue

        if case_type in ('new_domain', 'genuine_change', 'genuine_reappearance'):
            new_val, new_ms = today_val, None
        elif case_type == 'churn':
            new_val, new_ms = state_val, None
        else:                                       # new_disappearance with grace>0
            new_val, new_ms = state_val, current_date

        new_domains.append(domain);  new_qtypes.append(qtype)
        new_values.append(new_val);  new_missing.append(new_ms)
        new_reg.append(reg_dom)

    if keys_to_remove and old_state.num_rows > 0:
        composite = pc.binary_join_element_wise(
            old_state.column('domain'),
            old_state.column('query_type'),
            # null-byte separator — safe since domains never contain it. Must be
            # typed large_string to match the wide domain/query_type columns;
            # a bare str literal infers as `string` and the kernel has no
            # (large_string, large_string, string) overload.
            pa.scalar('\x00', type=LARGE_STR),
        )
        remove_arr = pa.array(
            [f"{d}\x00{qt}" for d, qt in keys_to_remove], type=LARGE_STR
        )
        keep_mask = pc.invert(pc.is_in(composite, value_set=remove_arr))
        filtered_old = old_state.filter(keep_mask)
    else:
        filtered_old = old_state

    if not new_domains:
        return filtered_old

    n = len(new_domains)
    new_rows = pa.table({
        'domain':             pa.array(new_domains,      LARGE_STR),
        'query_type':         pa.array(new_qtypes,       LARGE_STR),
        'value':              pa.array(new_values,       LARGE_LIST),
        'missing_since':      pa.array(new_missing,      pa.date32()),
        'registrable_domain': pa.array(new_reg,          LARGE_STR),
        'bucket':             pa.array([bucket] * n,     pa.int32()),
    })
    return pa.concat_tables([filtered_old, new_rows])


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

def _bucket_dir(bucket: int) -> str:
    return os.path.join(EVENTS_DIR, f'bucket={bucket}')


def _delta_files(bdir: str) -> list[str]:
    if not os.path.isdir(bdir):
        return []
    return sorted(
        os.path.join(bdir, f)
        for f in os.listdir(bdir)
        if f.startswith('delta-') and f.endswith('.parquet')
    )


def _delta_bytes(bdir: str) -> int:
    return sum(os.path.getsize(f) for f in _delta_files(bdir))


def compact_bucket(bucket: int) -> dict:
    """Merge base.parquet + all deltas into a new base.parquet.
    Crash-safe: writes to .tmp, validates row count, then atomically renames."""
    bdir   = _bucket_dir(bucket)
    deltas = _delta_files(bdir)
    base   = os.path.join(bdir, 'base.parquet')
    tmp    = base + '.tmp'

    to_read = ([base] if os.path.exists(base) else []) + deltas
    if not to_read:
        return {'skipped': True}

    bytes_before = sum(os.path.getsize(f) for f in to_read)
    t0 = time.time()
    files_sql = ', '.join(f"'{f}'" for f in to_read)

    con = duckdb.connect()
    con.execute(f"PRAGMA memory_limit='{MAIN_CON_MEMORY}';")
    con.execute(f"PRAGMA threads={MAIN_CON_THREADS};")
    try:
        # SELECT DISTINCT * deduplicates rows that were written twice due to
        # a crash between delta write and ingest_log update.
        con.execute(f"""
            COPY (
                SELECT DISTINCT
                    domain, registrable_domain, domain_reversed,
                    query_type, measurement_date, value, prev_value, bucket
                FROM read_parquet([{files_sql}])
                ORDER BY registrable_domain, domain_reversed, measurement_date
            ) TO '{tmp}' (FORMAT PARQUET, CODEC 'ZSTD', ROW_GROUP_SIZE 100000)
        """)
        new_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp}')").fetchone()[0]
        if new_count == 0:
            os.unlink(tmp)
            return {'skipped': True, 'reason': 'empty result'}

        os.replace(tmp, base)
        for d in deltas:
            os.unlink(d)

        bytes_after = os.path.getsize(base)
        duration    = time.time() - t0
        print(
            f"  [compact] bucket={bucket}: {len(to_read)} file(s) "
            f"({bytes_before / 1024 / 1024:.1f} MB) → 1 file "
            f"({bytes_after / 1024 / 1024:.1f} MB) in {duration:.1f}s"
        )
        return {
            'bucket': bucket, 'files_before': len(to_read),
            'bytes_before': bytes_before, 'bytes_after': bytes_after,
            'duration': duration,
        }
    finally:
        con.close()
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _buckets_over_threshold(threshold: int, candidates=None) -> list[tuple[int, int]]:
    """Returns [(delta_bytes, bucket), ...] sorted largest-first."""
    pool = candidates if candidates is not None else range(256)
    result = []
    for b in pool:
        sz = _delta_bytes(_bucket_dir(b))
        if sz >= threshold:
            result.append((sz, b))
    return sorted(result, reverse=True)


def maybe_compact(touched_buckets: set[int]) -> None:
    """Inline compaction: run after each day, up to MAX_COMPACTIONS_PER_DAY."""
    over = _buckets_over_threshold(COMPACTION_DELTA_BYTES, touched_buckets)
    if not over:
        return
    print(f"\n  [compact] {len(over)} bucket(s) over threshold; "
          f"compacting up to {MAX_COMPACTIONS_PER_DAY}.")
    for _, bucket in over[:MAX_COMPACTIONS_PER_DAY]:
        compact_bucket(bucket)


def final_compaction_pass() -> None:
    print("\n--- Final compaction pass ---")
    t_start = time.time()
    over = _buckets_over_threshold(FINAL_COMPACTION_DELTA_BYTES)
    if not over:
        print("  Nothing to compact.")
        return
    print(f"  {len(over)} bucket(s) over {FINAL_COMPACTION_DELTA_BYTES // 1024 // 1024} MB threshold.")

    total_before = total_after = compacted = 0
    for _, bucket in over:
        r = compact_bucket(bucket)
        if not r.get('skipped'):
            total_before += r['bytes_before']
            total_after  += r['bytes_after']
            compacted    += 1

    duration = time.time() - t_start
    print(
        f"  Final compaction done: {compacted} bucket(s), "
        f"{total_before / 1024 / 1024 / 1024:.2f} GB → "
        f"{total_after / 1024 / 1024 / 1024:.2f} GB "
        f"in {datetime.timedelta(seconds=int(duration))}"
    )


def run_standalone_compact(force: bool = False, workers: int = 1) -> None:
    threshold = 0 if force else COMPACTION_DELTA_BYTES
    candidates = [
        (sz, b)
        for b in range(256)
        for sz in [_delta_bytes(_bucket_dir(b))]
        if force or sz >= threshold
    ]
    candidates.sort(reverse=True)

    if not candidates:
        print("No buckets need compaction.")
        return

    label = "force" if force else f"over {threshold // 1024 // 1024} MB"
    print(f"Compacting {len(candidates)} bucket(s) ({label}), workers={workers}")

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(compact_bucket, b) for _, b in candidates]
            for f in as_completed(futs):
                f.result()
    else:
        for _, b in candidates:
            compact_bucket(b)


# ---------------------------------------------------------------------------
# Main ingest loop
# ---------------------------------------------------------------------------

def do_ingest(ingest_start: datetime.date, ingest_end: datetime.date) -> None:
    os.makedirs(BASE_DIR, exist_ok=True)

    main_con  = _main_con()
    query_con = _query_con()

    recovered_date = _bootstrap_if_needed(query_con)
    state_by_bucket = _load_state()

    last_done = _load_last_date()
    if last_done is None and recovered_date is not None:
        print(f"Ingest log absent — using derived last-processed date: {recovered_date}")
        last_done = recovered_date
        _save_last_date(recovered_date)
    if last_done and last_done >= ingest_start:
        resume = last_done + datetime.timedelta(days=1)
        print(f"Resuming from {resume} (last completed: {last_done})")
        ingest_start = resume

    if ingest_start > ingest_end:
        print("Nothing to do — already up to date.")
        return

    total_days   = (ingest_end - ingest_start).days + 1
    days_done    = 0
    recent_dur   = deque(maxlen=100)
    script_start = time.time()
    current_date = ingest_start

    while current_date <= ingest_end:
        day_start = time.time()
        print(f"\n--- Processing {current_date} ({days_done + 1}/{total_days}) ---")

        changed, touched, state_by_bucket = process_day(
            current_date, main_con, query_con, state_by_bucket
        )

        # Crash-safe: write the log only after both delta and state are committed.
        _save_last_date(current_date)

        duration = time.time() - day_start
        recent_dur.append(duration)
        days_done += 1
        days_left  = total_days - days_done
        avg        = sum(recent_dur) / len(recent_dur)
        eta_secs   = int(avg * days_left)

        print(f"\n{'=' * 50}")
        print(f"PROGRESS: {current_date}  ({changed:,} changes)")
        print(f"  • Day Duration       : {duration:.2f}s")
        print(f"  • Rolling Avg (100d) : {avg:.2f}s")
        print(f"  • Days Remaining     : {days_left}")
        print(f"  • Est. Time Left     : {datetime.timedelta(seconds=eta_secs)}")
        print(f"{'=' * 50}")

        if touched:
            maybe_compact(touched)

        current_date += datetime.timedelta(days=1)

    total_elapsed = datetime.timedelta(seconds=int(time.time() - script_start))
    print(f"\nDay loop complete. Total run time: {total_elapsed}")

    final_compaction_pass()

    print(f"\nDone. Total time including compaction: "
          f"{datetime.timedelta(seconds=int(time.time() - script_start))}")

    main_con.close()
    query_con.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="OpenINTEL email-record ingest pipeline (large-scale memory optimised)"
    )
    parser.add_argument(
        '--compact', action='store_true',
        help='Compact buckets over the delta-size threshold without ingesting.'
    )
    parser.add_argument(
        '--force-compact', action='store_true',
        help='Compact every bucket unconditionally.'
    )
    parser.add_argument(
        '--workers', type=int, default=1,
        help='Worker threads for standalone compaction (default: 1).'
    )
    args = parser.parse_args()

    if args.force_compact or args.compact:
        run_standalone_compact(force=args.force_compact, workers=args.workers)
    else:
        do_ingest(start_date, end_date)


if __name__ == '__main__':
    main()
