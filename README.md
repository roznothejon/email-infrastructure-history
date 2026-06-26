# Life of a Domain Name — Episode 2: Email

University of Twente research project (TScIT 45). Transforms OpenINTEL's daily DNS snapshots into a per-domain, event-based dataset for tracking email infrastructure changes over time. Covers MX records (provider mapping) and SPF records (sender authorization + security posture).

**Three independent layers:**

```
OpenINTEL S3  →  Event store (Parquet)  →  Streamlit explorer app
                 data/<source>/events/       app/app.py
```

Re-running analysis never requires re-fetching from S3. Adding a provider mapping never requires reprocessing events.

---

## Requirements

- Python 3.12+
- OpenINTEL S3 credentials (for ingest and historical analysis scripts)
- ~96 GB RAM for zone-file scale ingest; top-list scale runs on ~16 GB

---

## Dataset

The dataset used for this research can be found [here](https://zenodo.org/records/20933099).

## Setup

```bash
# Clone and create venv
python3.12 -m venv venv
source venv/bin/activate
pip install -r app/requirements.txt

# Install pipeline deps (not in app/requirements.txt)
pip install tldextract

# Credentials
cp .env.example .env
# Edit .env — fill OPENINTEL_KEY_ID and OPENINTEL_SECRET
```

`.env.example`:
```
OPENINTEL_KEY_ID=your-access-key-here
OPENINTEL_SECRET=your-secret-key-here
```

All scripts below assume `source venv/bin/activate` first.

---

## Ingest pipeline

`preprocessing/preprocess_parquet_v3.py` — production pipeline. Pulls daily OpenINTEL snapshots from S3, diffs against last-known state per bucket, appends change events to `data/<source>/events/`.

```bash
# Normal run — auto-resumes from last completed date
python preprocessing/preprocess_parquet_v3.py

# Compact buckets that exceed the delta-size threshold
python preprocessing/preprocess_parquet_v3.py --compact

# Force-compact every bucket (e.g. after a backfill)
python preprocessing/preprocess_parquet_v3.py --force-compact

# Parallel compaction
python preprocessing/preprocess_parquet_v3.py --force-compact --workers 8
```

**Resume behavior:** on first run with no local data, the first day's snapshot is treated as all-new arrivals. Subsequent runs pick up from `data/<source>/ingest_log.json`. If `state.parquet` is missing but events exist, it bootstraps from the event store automatically.

**Memory:** ~89 GB peak RSS at 200M-domain zone-file scale. Top-list scale (~3–5M domains) fits comfortably on a 16 GB machine.

Full design reference: `preprocessing/PIPELINE.md`.

---

## Streamlit app

### Local (venv)

```bash
source venv/bin/activate
streamlit run app/app.py
```

Opens at `http://localhost:8501`. Type any domain to see its MX/SPF history as an interactive timeline.

### Docker

```bash
# Build and run
docker compose up --build

# Detached
docker compose up -d --build
```

App available at `http://localhost:8501`. The `data/` directory is mounted read-only into the container — the image contains only the app code. Update the event store on the host and restart the container to pick up new data.

**Manual Docker run (no compose):**

```bash
docker build -t email-dns-app .
docker run -p 8501:8501 -v $(pwd)/data:/repo/data:ro email-dns-app
```

---

## Analysis scripts

All read from the local event store / state, not from S3 (except `*_historical.py` scripts).

```bash
# Top MX servers in current state
python scripts/analysis/top_records.py [--source toplists|zonefiles|both] [--top N]

# Top SPF mechanisms in current state
python scripts/analysis/top_spf.py [--source toplists|zonefiles|both] [--top N]

# MX provider distribution over time — samples S3 monthly (~25–50 min)
python scripts/analysis/top_mx_historical.py [--source both] [--top N]

# SPF mechanism distribution over time — samples S3 monthly (~25–50 min)
python scripts/analysis/top_spf_historical.py [--source both] [--top N]

# SPF adoption curves from event store (~10–30 min)
python scripts/analysis/spf_adoption_monthly.py [--source both] [--top-includes N] [--output-dir DIR]

# Event store health report (row counts, churn, volatility)
python scripts/analysis/dataset_metrics.py [--data-dir DIR] [--output FILE]

# Rebuild / extend provider mapping JSONs (run after *_historical.py)
python scripts/analysis/build_provider_candidates.py [--dry-run]
```

---

## Provider mappings

`data/mappings/mx_providers.json` — MX hostname → provider name  
`data/mappings/spf_providers.json` — SPF mechanism → provider name

Hand-curated. Edit directly to add or correct entries. No event reprocessing needed — mappings are joined at query time.

To bootstrap new entries from historical frequency reports:
1. Run `top_mx_historical.py` and `top_spf_historical.py` to produce report `.txt` files.
2. Update the paths at the top of `build_provider_candidates.py`.
3. Run `build_provider_candidates.py` — existing entries are never overwritten.

---

## Validation

```bash
cd scripts/validation

# Technical: random S3 records → check they're in the event store
python validate_completeness.py [--source toplists|zonefiles|both] [--n N] [--seed S] [-v]

# Technical: random events → re-fetch raw S3 day → assert values match
python validate_correctness.py [--source both] [--n N] [--seed S] [--include-disappearances] [-v]

# Performance: event-store query times vs. equivalent raw-S3 queries
python validate_performance.py [--dataset toplists|zonefiles] [--domains ...] [--s3-days N]

# Self-tests — prove validators reject bad data
python test_validate_detects_errors.py
python test_validate_correctness_detects_errors.py
```

---

## Repository layout

```
.
├── preprocessing/
│   ├── preprocess_parquet_v3.py   # production ingest pipeline
│   └── PIPELINE.md                # full design reference
├── app/
│   ├── app.py                     # Streamlit UI
│   ├── queries.py                 # data access layer (no Streamlit dep)
│   └── pages/about.py             # About / dataset stats page
├── scripts/
│   ├── analysis/                  # post-hoc analysis scripts
│   └── validation/                # three-level validation suite
├── data/
│   ├── mappings/                  # provider classification JSONs
│   ├── toplists/events/           # partitioned Parquet event store
│   └── zonefiles/events/          # same, for zone-file sources
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

---

## Data layout (brief)

Events are stored as append-only Parquet, partitioned into 256 buckets by `blake2b(registrable_domain) % 256`. All subdomains of a registrable domain land in the same bucket, enabling sub-second per-domain and suffix queries regardless of total dataset size.

Each bucket accumulates small daily delta files; periodic compaction merges them into a single sorted base file. State (`state.parquet`) holds the last-known value per `(domain, query_type)` and is replaced atomically each day.

See `preprocessing/PIPELINE.md` for the full storage design, crash safety model, and memory budget breakdown.

---
