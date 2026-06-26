# Preprocessing Pipeline — Design & Implementation Reference

`preprocess_parquet_v3.py` — per-bucket diff edition, zone-file ready, large-scale memory optimised.

---

## Table of Contents

1. [Purpose](#1-purpose)
2. [Source Data](#2-source-data)
3. [Technology Stack](#3-technology-stack)
4. [Storage Architecture](#4-storage-architecture)
5. [Bucketing Design](#5-bucketing-design)
6. [Per-Day Pipeline](#6-per-day-pipeline)
7. [Case Classification](#7-case-classification)
8. [Crash Safety](#8-crash-safety)
9. [State Management](#9-state-management)
10. [Compaction](#10-compaction)
11. [Bootstrap Recovery](#11-bootstrap-recovery)
12. [Performance](#12-performance)
13. [Configuration Reference](#13-configuration-reference)

---

## 1. Purpose

OpenINTEL produces daily DNS snapshots: for each measured domain, a row saying "on this date, the MX record was X and the TXT record was Y." These snapshots are enormous (hundreds of millions of rows per day at zone-file scale) and duplicative — most domains don't change day to day.

The pipeline transforms those snapshots into an **event store**: an append-only, change-only dataset where each row records a *transition* (`prev_value → value`) for one domain on one date. This makes longitudinal queries fast — instead of scanning 3600 daily snapshots to trace a domain's history, you read one small bucket file containing only the changes.

The output is independent of OpenINTEL's storage layout and format. Once built, the event store never needs to touch S3 again.

---

## 2. Source Data

OpenINTEL measures active DNS daily since 2016. Two source categories are supported:

**Top-lists** — union of five commercial top-1M domain lists (Umbrella, Tranco, Radar, Alexa, Majestic). S3 path pattern:
```
s3://openintel/catalog/warehouse/fdns/data/source={source}/date={date}/*.parquet
```

**Zone files** — zone transfers from ccTLD/gTLD registries (ee, fr, gov, li, nu, se, sk, etc.). S3 path pattern:
```
s3://openintel/category=fdns/type=warehouse/source={source}/year={Y}/month={M}/day={D}/*.parquet
```

Each raw Parquet file has columns including `query_name`, `query_type`, `mx_address`, `txt_text`, and others. The pipeline keeps only:
- `query_type = 'MX'` with non-null `mx_address`
- `query_type = 'TXT'` with non-null `txt_text`

Everything else is filtered at fetch time, before any data lands in local memory.

---

## 3. Technology Stack

### DuckDB

Used as the query engine throughout — not as a database. All DuckDB connections are in-memory (`duckdb.connect()` with no path argument). DuckDB handles:
- S3 reads via the `aws` extension (streaming Parquet over HTTPS, parallel)
- SQL aggregation and joins during the diff
- Writing delta Parquet files via `COPY ... TO ... (FORMAT PARQUET, PARTITION_BY (bucket))`
- Compaction queries (`SELECT DISTINCT * ... ORDER BY ...`)

DuckDB is not used for persistent storage. The on-disk format is plain Parquet files. This means the data survives DuckDB version upgrades and can be read by any Parquet-aware tool.

Two separate DuckDB connections are maintained per run:

| Connection | Memory | Role |
|---|---|---|
| `main_con` | 24 GB | S3 fetching, aggregation, delta writes |
| `query_con` | 8 GB | Per-bucket diff joins |

Keeping them separate prevents the diff queries from competing with large S3 fetch buffers.

Per-thread fetch connections (`_fetch_con`) are also created lazily via `threading.local()`, one per fetch worker. Each loads the S3 credentials once.

### PyArrow

Used for all in-memory data transport between pipeline stages. The state table, today's aggregated snapshot, and all intermediate results live as `pa.Table` objects. Key reasons:

- **Zero-copy slicing** — `_partition_by_bucket` uses `pa.Table.take()` to slice a table into 256 per-bucket views without copying column buffers. This is O(N) total for all 256 slices.
- **Direct DuckDB integration** — Arrow tables are registered into DuckDB via `con.register()` and unregistered immediately after use, so DuckDB treats them as virtual tables without copying data.
- **Vectorized filtering** — `_apply_actionable_to_state` uses `pyarrow.compute` (`pc.binary_join_element_wise`, `pc.is_in`, `pc.invert`) to filter old state rows via a bitmask rather than a Python loop over state rows.

**Large-type / int32-offset policy.** Arrow's `string` and `list<...>` types use int32 offsets, capping a single contiguous array at ~2 GB of string bytes or 2³¹ list child elements. At zone-file scale, a fresh-start or heavy-change day buffers ~all domains into one `events_enriched` table before writing — large enough to overflow that cap. All in-memory **state** and **event** tables (`_EMPTY_STATE`, `_EMPTY_BUCKET`, `events_enriched`, state's `new_rows`) therefore use `large_string` / `large_list(large_string)` (`LARGE_STR` / `LARGE_LIST` in `preprocess_parquet_v3.py`). DuckDB reads these natively and still writes plain `VARCHAR`/`LIST` Parquet, so on-disk logical types and every downstream reader (`app/queries.py`, compaction) are unaffected. Because DuckDB→Arrow conversions always emit the narrow types, every DuckDB-produced table that feeds the in-memory state (state load, bootstrap rebuild) is passed through `_cast_to_schema` first. The narrow `string`/`list` types remain safe and unchanged where an array is bounded well under 2 GB by construction — e.g. `_compute_reg_and_bucket`'s per-`CHUNK_SIZE` (1M-row) arrays. One ceiling remains even with large types: a single Arrow table is capped at 2³¹-1 *rows* regardless of offset width; a single day producing more events than that would need to be written in row-batches instead of one `events_enriched` table.

### tldextract

Computes the eTLD+1 (registrable domain) from each queried hostname using the Mozilla Public Suffix List. `utwente.nl`, `mail.utwente.nl`, and `student.utwente.nl` all return `utwente.nl` as the registrable domain.

Called once per unique `query_name` within each chunk (see §6 Step 3). At 3M unique domains per day this saves ~2–3 seconds compared to calling it for every row.

Python's built-in `hash()` is never used for bucket assignment — it is randomised per process. All hashing uses `hashlib.blake2b` with a fixed digest size, ensuring the same domain always lands in the same bucket across runs and machines.

---

## 4. Storage Architecture

### Event Store

```
data/<dataset>/
  events/
    bucket=0/
      base.parquet              ← compacted bulk history (sorted, deduped)
      delta-20240315-{uuid}.parquet  ← one file per day that touched this bucket
    bucket=1/
    ...
    bucket=255/
  state.parquet                 ← last-known value per (domain, query_type)
  ingest_log.json               ← {"last_date": "YYYY-MM-DD"}
```

**Why Parquet, not a database?** The previous design used a single growing DuckDB file. It hit a wall at ~260 GB with top-lists alone. A monolithic file has no partial-read capability — querying one domain forces a scan of the entire file or relies on an index that itself grows unboundedly. Zonefiles (50M+ domains) would hit that wall much sooner.

Parquet-as-storage with DuckDB-as-engine separates durability from querying. Each bucket is an independent set of files. A query for `utwente.nl` opens exactly one bucket directory (~1 GB at scale), DuckDB uses row-group statistics for pushdown, and the query returns sub-second.

**Event schema:**

| Column | Type | Notes |
|---|---|---|
| `domain` | `VARCHAR` | Queried hostname, e.g. `mail.utwente.nl` |
| `registrable_domain` | `VARCHAR` | eTLD+1, e.g. `utwente.nl` |
| `domain_reversed` | `VARCHAR` | `nl.utwente.mail` — enables prefix-range queries |
| `query_type` | `VARCHAR` | `'MX'` or `'TXT'` |
| `measurement_date` | `DATE` | Date the change was observed |
| `value` | `VARCHAR[]` | New value as sorted list; `NULL` = domain disappeared |
| `prev_value` | `VARCHAR[]` | Previous value; `NULL` = first appearance |
| `bucket` | `INTEGER` | Redundant with directory path; enables DuckDB predicate pushdown without directory scanning |

Values are stored as **sorted lists** (`VARCHAR[]`), never as concatenated strings. This makes equality comparison work correctly — two sets of MX records are equal if and only if their sorted lists are equal. `list_sort(list(val))` during aggregation handles deduplication and ordering.

`domain_reversed` stores the label-reversed hostname. DuckDB cannot efficiently do suffix queries (`WHERE domain LIKE '%.utwente.nl'`) but can efficiently do prefix queries (`WHERE domain_reversed LIKE 'nl.utwente.%'`). Storing the reversal at write time keeps query-time code simple.

### State File

`state.parquet` holds the **last known value** for every (domain, query_type) pair currently in the measured dataset. It is the right-hand side of each day's diff. Schema:

| Column | Notes |
|---|---|
| `domain` | PK component |
| `query_type` | PK component |
| `value` | `NULL` while domain is in grace period |
| `missing_since` | `NULL` unless domain is in grace period |
| `registrable_domain` | Cached eTLD+1 — avoids re-computing PSL for domains that vanish |
| `bucket` | Enables per-bucket join filtering |

State is written as a single flat Parquet file via atomic rename (`.tmp` → `os.replace`). There is no WAL, no index, no incremental append — the entire file is replaced on every day's commit. At 3M rows this takes ~1–2 seconds. The simplicity eliminates an entire class of corruption bugs.

State rows are deleted (not nulled) when a domain disappears permanently (grace expired or GRACE_PERIOD_DAYS = 0). This prevents unbounded growth during multi-year backfills.

### Ingest Log

`ingest_log.json` contains a single key: `{"last_date": "YYYY-MM-DD"}`. It is the only thing the pipeline reads to decide where to resume. It is written last in each day's commit sequence — after both the delta and state are safely on disk. A crash before the log write means the day is replayed on restart; duplicates produced by the replay are harmless and removed by compaction.

---

## 5. Bucketing Design

Every domain is assigned to one of 256 buckets:

```python
bucket = int.from_bytes(
    hashlib.blake2b(registrable_domain.lower().encode(), digest_size=4).digest(),
    'big'
) % 256
```

The key properties:

**Registration-domain bucketing.** The hash is on the *registrable domain* (eTLD+1), not the full queried hostname. This means `utwente.nl`, `mail.utwente.nl`, and `student.utwente.nl` all land in the same bucket. A query for "all email records for any subdomain of `utwente.nl`" touches exactly one bucket.

**Determinism.** `blake2b` with a fixed digest size always produces the same bucket for the same input, regardless of Python version, platform, or environment variables. Python's built-in `hash()` is seeded randomly per process since Python 3.3 and cannot be used for this.

**256 buckets.** At top-list scale (~3M domains), each bucket holds ~12K domains. At zone-file scale (~50M domains), each holds ~200K. This keeps per-bucket DuckDB joins small (~20 MB hash tables) regardless of total dataset size. The number 256 is a constant embedded in the directory structure — changing it requires a full dataset rebuild.

---

## 6. Per-Day Pipeline

Each day runs `process_day()`, which executes seven sequential steps.

### Step 1: Fetch (parallel S3 reads)

All configured sources are fetched concurrently using a `ThreadPoolExecutor`. Each worker calls `fetch_source()`, which runs a DuckDB query over S3 using that thread's own connection (`_fetch_con()`, lazily initialised via `threading.local`).

The fetch query filters at the source:
```sql
SELECT query_name, query_type, COALESCE(mx_address, txt_text) AS val
FROM read_parquet('s3://...')
WHERE (query_type = 'MX'  AND mx_address IS NOT NULL)
   OR (query_type = 'TXT' AND txt_text   IS NOT NULL)
```

This discards the vast majority of columns and rows before the data crosses the network → memory boundary. The result is an Arrow table.

Results are inserted into a temporary DuckDB table `raw_today` under `_insert_lock` — a threading lock is needed because DuckDB's `register()` + `INSERT` is not thread-safe for concurrent writes to the same connection.

If a source has no data for that date (missing partition on S3), the exception is caught and the source is skipped silently.

### Step 2: Aggregate

```sql
SELECT query_name, query_type, list_sort(list(val)) AS value
FROM (SELECT DISTINCT query_name, query_type, val FROM raw_today)
GROUP BY query_name, query_type
```

This produces one row per `(domain, query_type)` with `value` as a sorted, deduplicated list of all observed values. Multiple sources can report different values for the same domain (e.g., Umbrella and Tranco both measure `google.com`) — the union is taken. Sorting is mandatory for correct equality comparison during the diff.

### Step 3: Registrable-domain computation

`_compute_reg_and_bucket()` processes `today_agg`'s `query_name` column in slices of `CHUNK_SIZE` (default 1 M) rows. Within each slice, a local dict deduplicates `tldextract` calls so MX and TXT rows for the same domain share one PSL lookup. Both the string list and the local dict are freed after each slice, keeping peak Python allocation at O(`CHUNK_SIZE`) regardless of total row count. Results accumulate as compact Arrow arrays and are attached to `today_agg` via `append_column`, which is zero-copy for the existing columns.

This step is done **before** the diff so that both sides of each per-bucket join have the `bucket` column pre-computed.

### Step 4: Pre-partition both sides

`_partition_by_bucket()` scans the `bucket` column once, builds a dict of row indices per bucket, and calls `pa.Table.take()` once per bucket to produce a zero-copy slice. This runs in O(N) total.

The same function runs on the in-memory state dict (already partitioned from the previous day, so this is free).

Both today's data and state are now split into 256 per-bucket Arrow slices ready for the diff loop. No DuckDB query is needed to filter state by bucket.

### Step 5: Per-bucket diff (PASS 1 — collect only)

For each of the 256 buckets:

1. Register today's slice and state slice as virtual tables in `query_con`.
2. Run a `FULL OUTER JOIN` on `(query_name/domain, query_type)`.
3. The `CASE` expression classifies each row into one of eight cases (see §7).
4. The `WHERE` clause filters out `no_change` and `in_grace` rows before the result is materialised — only actionable rows reach Python.
5. Unregister both virtual tables immediately to free `query_con`'s memory.

Event rows are appended to Python lists (`ev_domains`, `ev_values`, etc.). This accumulates all events across all 256 buckets before anything is written to disk. The reason is crash safety: if we wrote each bucket's delta immediately, a crash mid-loop would leave partial state with no way to distinguish "was this bucket already written?".

### Step 6: Write delta Parquet files

After all 256 buckets have been diffed, all events are assembled into a single Arrow table and written via DuckDB's `COPY ... TO ... (FORMAT PARQUET, PARTITION_BY (bucket))`. DuckDB partitions the output by bucket automatically, writing one `delta-YYYYMMDD-{uuid}.parquet` file per touched bucket.

`OVERWRITE_OR_IGNORE TRUE` prevents conflicts if a delta file with the same UUID already exists (extremely unlikely but possible on retry).

The `uuid` suffix in the filename means a day re-run produces a new file rather than overwriting the previous one. Duplicates are deduped at compaction time by `SELECT DISTINCT *`.

**Compression: ZSTD, not Snappy.** All Parquet writes in this pipeline (delta files, compacted `base.parquet`, `state.parquet`) use `CODEC 'ZSTD'` / `compression='zstd'`. Snappy was the original default (faster decode, but worse ratio). Measured on the full event store (both sources, 514 files): **19.85 GB (Snappy) → 14.73 GB (ZSTD), a 1.35x reduction (~26% smaller)**:

| Dataset | Snappy | ZSTD |
|---|---|---|
| `toplists/events/` | 17 GB | 13 GB |
| `toplists/state.parquet` | 304.8 MB | 205.2 MB |
| `zonefiles/events/` | 1.7 GB | 1.2 GB |
| `zonefiles/state.parquet` | 3.0 MB | 2.1 MB |

Codec is per-file Parquet metadata — DuckDB/Arrow/pandas auto-detect it on read, so mixed-codec datasets (some buckets Snappy, some ZSTD) are never a correctness issue, only relevant if doing a rolling migration. The live `data/toplists` and `data/zonefiles` event stores were fully migrated to ZSTD in one pass (verified via exact row-count + full content diff against the Snappy originals before the swap, then the Snappy copies were deleted).

### Step 7: Update state, persist, and return

The in-memory state dict is patched bucket by bucket via `_apply_actionable_to_state()`. This function:

1. Iterates the actionable rows (small — at most ~N/256 changed rows per bucket).
2. For each changed domain, marks its (domain, query_type) key for removal from the old state slice.
3. Constructs new state rows for domains that are now live, in grace, or updated.
4. Uses a vectorized Arrow filter (via `pc.is_in` on a composite key column) to drop the old rows — no Python loop over the potentially large old state.
5. Concatenates the filtered old state with the new rows.

The full updated state dict is written atomically to `state.parquet` via `pq.ParquetWriter`, streaming one bucket slice at a time into a `.tmp` file before `os.replace`. This avoids a full O(state) concat allocation — concatenating all 256 slices into one Arrow table before writing would double the state footprint momentarily (~35 GB at 200 M-domain scale).

The caller (`do_ingest`) then writes `ingest_log.json` — always last.

---

## 7. Case Classification

For every `(domain, query_type)` pair that appears in either today's data or the state, the diff assigns one of eight cases:

| Case | Condition | Action |
|---|---|---|
| `new_domain` | In today, not in state | Event: `NULL → value`. Add to state. |
| `genuine_change` | In both, value differs | Event: `old → new`. Update state. |
| `no_change` | In both, value identical, not in grace | No event. No state change. Excluded by WHERE. |
| `new_disappearance` | In state (live), not in today | If `GRACE=0`: event `value → NULL`, delete state row. If `GRACE>0`: set `missing_since`, no event yet. |
| `in_grace` | In state with `missing_since` set, still within grace window | No action. Excluded by WHERE. |
| `grace_expired` | In state with `missing_since` set, grace window elapsed | Event dated to `missing_since` (not today). Delete state row. |
| `churn` | Reappeared within grace with same value | Reset `missing_since` to NULL. No event. This was toplist noise. |
| `genuine_reappearance` | Reappeared after grace, or with different value | Two events: disappearance at `missing_since`, reappearance at today. Add to state. |

**Why grace periods?** Top-list membership fluctuates — a domain near the boundary of the Umbrella top-1M may appear on Monday, vanish on Tuesday (dropped to rank 1,000,001), and reappear on Wednesday. Without a grace period, this generates two spurious events per day for thousands of domains. `GRACE_PERIOD_DAYS = 7` suppresses these if the domain returns with the same value within the window. The current setting is `0` (disabled) since the zonefiles dataset uses zone transfer data, which is authoritative and doesn't have this noise.

**Grace event dating.** When grace expires, the disappearance event is backdated to `missing_since`, not today. This is important for historical accuracy — the domain was gone since `missing_since`; we are only *confirming* it today. The consumer should not see a disappearance event on the wrong date.

---

## 8. Crash Safety

The commit order within each day is strict:

```
1. Diff all 256 buckets → accumulate events in memory
2. Write delta Parquet files to disk
3. Update in-memory state dict, write state.parquet  (.tmp → os.replace)
4. do_ingest() writes ingest_log.json  (.tmp → os.replace)
```

**Crash after step 1, before step 2:** Nothing was written. The day is re-run in full on restart. Clean.

**Crash after step 2, before step 3:** Delta files exist but state is behind. On restart: the ingest log has not been updated, so the day is re-run. The diff produces the same events again. Duplicate delta files with different UUIDs are written. Compaction deduplicates via `SELECT DISTINCT *`. State ends up correct.

**Crash after step 3, before step 4:** Delta and state are both written, but the log is not. On restart: the day is re-run. The diff produces zero changes (state already reflects today). No new delta is written. The log is updated. Clean.

**Crash after step 4:** Day is complete. Restart picks up from the next day.

All file writes go through `.tmp` → `os.replace()`. On POSIX, `os.replace` is atomic at the filesystem level. A partial write to `.tmp` is never visible as the final file.

---

## 9. State Management

### Why a flat Parquet file, not a database?

At 3M–50M rows, a traditional row-store database (SQLite, DuckDB persistent file) would need an index on `(domain, query_type)` to make per-domain lookups fast. But this pipeline doesn't do per-domain lookups on state — it does bulk batch operations (one join per bucket). An index adds write overhead without providing any benefit.

The flat Parquet file is written as a full replacement once per day. At 3M rows, this costs ~1–2 seconds. This is cheaper than the growing `DELETE + INSERT` overhead of an indexed table, which was measured to grow with dataset size in the v1 design.

### In-memory state across days

The state dict (`state_by_bucket: dict[int, pa.Table]`) is loaded once at startup and kept in memory for the entire run. Day N+1's diff reads from the in-memory dict, not from disk. This saves one Parquet read per day (~40 seconds at 17M rows). The `_save_state` write at the end of each day is the crash-safe checkpoint.

### State bootstrapping

If `state.parquet` is missing but the event store exists (e.g., the file was deleted, or the pipeline is being moved to a new machine), `_bootstrap_if_needed()` reconstructs state from the events before ingest begins:

```sql
SELECT domain, query_type,
       arg_max(value, measurement_date) AS value,
       CASE WHEN arg_max(value, measurement_date) IS NULL
            THEN MAX(measurement_date) ELSE NULL END AS missing_since,
       ANY_VALUE(registrable_domain) AS registrable_domain,
       CAST(ANY_VALUE(bucket) AS INTEGER) AS bucket,
       MAX(measurement_date) AS _max_date
FROM read_parquet([...bucket files...])
GROUP BY domain, query_type
```

`arg_max(value, measurement_date)` returns the value associated with the latest event date — the last known state. This is a GROUP BY (not a window function) and is significantly faster than `ROW_NUMBER() OVER (PARTITION BY ...)` on large datasets.

The bootstrap also derives the last-processed date (`MAX(measurement_date)` across all events) and, if `ingest_log.json` is also missing, writes the log so future restarts don't trigger a rebuild.

---

## 10. Compaction

As daily delta files accumulate, each bucket directory grows from one file per day. A bucket touched for 3650 days has 3650 delta files — small individually but slow to open and stat. Compaction merges them.

### Inline compaction

After each day's commit, `maybe_compact()` checks whether any touched bucket's total delta size exceeds `COMPACTION_DELTA_BYTES` (500 MB). If so, it compacts up to `MAX_COMPACTIONS_PER_DAY` (16) buckets to avoid stalling ingest.

### Final compaction pass

At the end of a backfill run, `final_compaction_pass()` applies a lower threshold (`FINAL_COMPACTION_DELTA_BYTES = 10 MB`) to mop up small accumulations across all 256 buckets.

### Standalone compaction

Can be triggered manually:
```
python preprocess_parquet_v3.py --compact           # over 500 MB threshold
python preprocess_parquet_v3.py --force-compact     # all buckets
python preprocess_parquet_v3.py --force-compact --workers 8
```

Multi-worker compaction runs bucket jobs in parallel via `ThreadPoolExecutor`.

### Compaction safety

```
1. Read base.parquet (if present) + all delta files
2. Write SELECT DISTINCT * ... ORDER BY ... to base.parquet.tmp
3. Validate: read the new file, check row count > 0
4. os.replace(tmp, base.parquet)
5. Delete delta files
```

If a crash occurs between steps 2 and 5, the `.tmp` file is cleaned up on the next run (finally block). If it crashes between 4 and 5, the base is already updated — the remaining delta files contain data already in the base, so they're safe to re-compact (DISTINCT handles duplicates). The old base never exists at the same time as a partial new base.

The `ORDER BY registrable_domain, domain_reversed, measurement_date` sort on compaction is what enables DuckDB's row-group statistics to give useful min/max bounds for predicate pushdown. Without this ordering, the row groups would contain random domains and the statistics would be useless.

---

## 11. Bootstrap Recovery

Four startup scenarios:

| `state.parquet` | `ingest_log.json` | Behaviour |
|---|---|---|
| Present | Present | Normal resume from log date + 1 |
| Present | Missing | Fresh ingest from `start_date` — user should supply correct `--start` |
| **Missing** | **Present** | Rebuild state from events; log is authoritative for resume date |
| **Missing** | **Missing** | Rebuild state from events; derive last date from events; write log |

The bootstrap is transparent — it runs before `_load_state()` and writes `state.parquet`, so the rest of `do_ingest()` sees no difference.

---

## 12. Performance

### Architectural history

The earliest version ran one global `FULL OUTER JOIN` between today's snapshot and the full state table. DuckDB built a ~200 MB hash table for the join. This worked at top-list scale but crashed at day ~1462 of a backfill as the state grew — at zone-file scale (50M domains) the hash table would exceed available RAM far sooner.

The current design runs 256 small joins, one per bucket. Each join sees at most N/256 rows on each side — roughly 12K rows per bucket at 3M domain scale, 200K at 50M. The hash table per join is ~20 MB regardless of total dataset size.

### Expected timing (per day)

| Phase | Top-lists (3–5M domains) | With zone files (50M domains) |
|---|---|---|
| Fetch | ~2–3s | ~2–3s (parallel, I/O bound) |
| Aggregation | ~1–2s | ~5–10s |
| Reg-domain | ~4–6s | ~30–60s (linear in unique domains) |
| Diff | ~5–10s | ~5–10s (**constant** — bounded by N/256) |
| Delta write | ~1–2s | ~5–10s |
| State upsert | ~5–10s | ~5–10s (**constant** — same reasoning) |
| **Total** | **~20–35s** | **~50–100s** |

The diff and state upsert phases do **not grow** with dataset size. Adding zone files increases total per-day time by ~30–60 seconds of reg-domain computation, not by hundreds of seconds of join time. This was the design goal.

### Memory usage

| Resource | Usage |
|---|---|
| `main_con` | 24 GB cap; in practice ~2–5 GB for top-lists |
| `query_con` | 8 GB cap; per-bucket joins use ~20–200 MB each |
| State in memory | ~640 MB at 3.7M rows; ~35 GB at 200M rows |
| Per-thread fetch connections | ~100 MB each, 5–7 threads |
| Reg-domain chunk (transient) | ~150 MB per chunk (strings + local cache); constant regardless of N |

Peak process RSS scales primarily with state size. At 200M domains (~35 GB state), worst-case peak is ~89 GB — dominated by state + today's aggregated snapshot coexisting during the diff. See the script docstring for the full breakdown.

---

## 13. Configuration Reference

All configuration is at the top of the script:

```python
TOPLIST_SOURCES  = ["umbrella", "tranco", "radar", "alexa", "majestic"]
ZONEFILE_SOURCES = ["ee", "fr", "gov", "li", "nu", "se", "sk"]

start_date = datetime.date(2016, 1, 22)
end_date   = datetime.date(2026, 1,  1)

DATA_DIR   = 'data/zonefiles'   # change to 'data/toplists' for top-list run

MAIN_CON_MEMORY  = '24GB'
MAIN_CON_THREADS = 6
STATE_CON_MEMORY = '8GB'

COMPACTION_DELTA_BYTES       = 500 * 1024 * 1024   # inline trigger
FINAL_COMPACTION_DELTA_BYTES =  10 * 1024 * 1024   # post-backfill trigger
MAX_COMPACTIONS_PER_DAY      = 16

GRACE_PERIOD_DAYS = 0   # set to 7 for top-list noise suppression

CHUNK_SIZE = 1_000_000  # rows per slice in _compute_reg_and_bucket
```

`DATA_DIR` is relative to the repository root. Changing it to point at a different directory is the only change needed to run a completely isolated second dataset.

`GRACE_PERIOD_DAYS = 0` means every disappearance is recorded immediately. Set to a positive value for top-list sources where churn noise is significant.

`MAX_COMPACTIONS_PER_DAY` caps inline compaction during backfill. Without this cap, a backfill that touches many hot buckets could spend more time compacting than ingesting.

`CHUNK_SIZE` controls how many rows `_compute_reg_and_bucket` processes per iteration. Reducing it lowers peak Python memory at the cost of more tldextract calls (domains spanning chunk boundaries are processed twice). The default of 1M keeps per-chunk allocation under ~150 MB.
