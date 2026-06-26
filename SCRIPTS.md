# Scripts reference

Quick reference for every script in the repo: what it does, why it exists, how to run it.
For the deep design of the ingest pipeline itself, see `preprocessing/PIPELINE.md` — this doc stays one level up.

All scripts run inside the project venv: `source venv/bin/activate` first.

## Pipeline

### `preprocessing/preprocess_parquet_v3.py`
Production ingest pipeline. Pulls daily OpenINTEL snapshots from S3, diffs against last-known state per bucket, appends change events to `data/<source>/events/`, rewrites `state.parquet`. Memory-optimized for 200M-domain zone-file scale (~89GB peak RSS). Run daily/backfill to keep the event store current. Details: `preprocessing/PIPELINE.md`.

## App

### `app/app.py`
Streamlit entry point. Lets a user type a domain, shows its MX/SPF history as an interactive timeline (Plotly), explains SPF posture in plain language. Run with `streamlit run app/app.py`.

### `app/pages/about.py`
Streamlit secondary page (About/FAQ/Metrics). Shows dataset coverage stats and MX/SPF provider distribution charts, pulled live via `queries.py`. Auto-discovered by Streamlit's multipage routing — no separate invocation.

### `app/queries.py`
Data access layer, no Streamlit dependency by design (so it's testable/reusable). Two public entry points: `get_domain_history(domain, source)` and `get_suffix_domains(reg_dom)`. Joins event-store Parquet against the provider mapping JSONs at query time. Imported by `app.py` and `about.py`, not run directly.

## Analysis (`scripts/analysis/`)

These all read from the already-built event store / state, not from S3 (except the two `*_historical.py` scripts) — cheap to re-run, no pipeline rebuild needed.

### `top_records.py`
Most common current MX servers, read from `state.parquet` (current values, not change history). `python scripts/analysis/top_records.py [--source toplists|zonefiles|both] [--top N] [--min-count M]`.

### `top_spf.py`
Same idea for SPF mechanisms, grouped by mechanism type, from `state.parquet`. `python scripts/analysis/top_spf.py [--source ...] [--top N] [--min-count M]`.

### `top_mx_historical.py`
Most common MX servers *over time* — samples the 15th of every month 2016→present directly from OpenINTEL S3 (not from local state), so it catches providers that have since disappeared. Slow: ~25-50min for `--source both` (1500 S3 reads). `python scripts/analysis/top_mx_historical.py [--source ...] [--top N] [--min-count M]`.

### `top_spf_historical.py`
SPF equivalent of the above — monthly S3 sampling since 2016. Same runtime cost, same reasoning (retired services/policies not in current state).

### `spf_adoption_monthly.py`
Reconstructs per-domain SPF state at the end of each calendar month from the event store (not S3), tracks adoption of SPF include categories and top-N specific includes over time. Outputs Parquet (for plotting) + a `.txt` report. Used to feed adoption-curve charts. `python scripts/analysis/spf_adoption_monthly.py [--source ...] [--top-includes N] [--output-dir DIR]`. ~10-30min.

### `dataset_metrics.py`
Health/shape report on the event store: event-type breakdown, unique domain counts, churn (disappear+reappear within N days), ephemeral domains, optimal grace-period suppression window, events-per-day, MX/TXT split, top volatile domains. Used to sanity-check the dataset and tune grace-period logic in the pipeline. `python scripts/analysis/dataset_metrics.py [--data-dir DIR] [--output FILE]`.

### `build_provider_candidates.py`
Merges hand-curated MX/SPF provider name guesses (in-script `MX_KNOWN`/dicts) plus frequency-report data into `data/mappings/{mx,spf}_providers.json`. Existing entries always win — only fills gaps. This is how the provider mapping tables get bootstrapped/extended; no event reprocessing needed afterward. `python scripts/analysis/build_provider_candidates.py [--dry-run]`. Needs the historical report `.txt` files (produced by the two `*_historical.py` scripts above) at the paths hardcoded near the top of the script.

## Validation (`scripts/validation/`)

Implements the three-level validation approach from the proposal (technical / event / performance). Re-runnable, no state mutation.

### `validate_completeness.py`
Technical validation: picks random raw S3 records, checks they made it into the event store at all (no silent drops at ingest). `python validate_completeness.py [--source toplists|zonefiles|both] [--n N] [--seed S] [-v]`.

### `validate_correctness.py`
Technical validation: picks random *events* from the store, re-fetches+re-aggregates the corresponding raw S3 day independently, asserts the stored value matches exactly. Disappearance events are checked inverted (domain should be absent from all sources that day). `python validate_correctness.py [--source ...] [--n N] [--seed S] [--include-disappearances] [-v]`.

### `validate_performance.py`
Performance validation: times event-store queries (exact + suffix) against equivalent raw-S3 queries, to quantify the speedup the Parquet/bucket layout buys. `python validate_performance.py [--dataset toplists|zonefiles] [--domains ...] [--s3-source ...] [--s3-days N]`.

### `test_validate_detects_errors.py`, `test_validate_correctness_detects_errors.py`
Proof-of-concept self-tests for the two validators above — feed each validator known-good and deliberately-hallucinated samples (fake domain, fake value) and assert it fails the bad ones. Exists to prove the validators aren't rubber-stamping everything. Run directly with no args; exits nonzero on assertion failure.
