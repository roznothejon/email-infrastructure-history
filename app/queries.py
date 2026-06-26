"""
Data access for the email infrastructure history app.
Two public entry points: get_domain_history(domain, source), get_suffix_domains(reg_dom).
No Streamlit dependency — caching wrappers live in app.py.
"""
import hashlib
import json
import re
from datetime import date
from pathlib import Path

import duckdb
import tldextract

REPO = Path(__file__).resolve().parent.parent


def _load_mapping(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


MX_MAP: dict = _load_mapping(REPO / "data/mappings/mx_providers.json")
SPF_MAP: dict = _load_mapping(REPO / "data/mappings/spf_providers.json")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bucket(reg_dom: str) -> int:
    key = (reg_dom or "").lower().encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=4).digest(), "big") % 256


def _discover_sources() -> dict[str, Path]:
    data_dir = REPO / "data"
    found = {}
    for p in sorted(data_dir.iterdir()):
        if p.is_dir() and (p / "events").is_dir() and p.name != "mappings":
            found[p.name] = p / "events"
    return found


SOURCES: dict[str, Path] = _discover_sources()


def _dataset_cutoff(source: str | None) -> date:
    sources = [source] if source else list(SOURCES.keys())
    best = None
    for s in sources:
        log = REPO / "data" / s / "ingest_log.json"
        try:
            last = json.loads(log.read_text()).get("last_date")
            if last:
                d = date.fromisoformat(last)
                if best is None or d > best:
                    best = d
        except (FileNotFoundError, ValueError, KeyError):
            pass
    return best or date.today()


def _parquet_globs(bucket: int, source: str | None = None) -> list[str]:
    bases = [SOURCES[source]] if source else list(SOURCES.values())
    result = []
    for base in bases:
        d = base / f"bucket={bucket}"
        if d.exists() and any(d.glob("*.parquet")):
            result.append(str(d / "*.parquet"))
    return result


def _fetch_events(glob: str, where: str) -> list[tuple]:
    con = duckdb.connect()
    sql = (
        f"SELECT domain, query_type, measurement_date, value "
        f"FROM read_parquet('{glob}') WHERE {where} "
        f"ORDER BY domain, query_type, measurement_date"
    )
    return con.execute(sql).fetchall()


def _extract_spf(values: list[str] | None) -> str | None:
    if not values:
        return None
    stripped = [v.strip('"') for v in values]
    # RFC 7208 §3.3: SPF may be split across multiple TXT strings; join them.
    joined = " ".join(stripped)
    if re.match(r"v=spf1", joined, re.IGNORECASE):
        return joined
    # Fallback: single-element match (handles non-SPF TXT records mixed in)
    for v in stripped:
        if re.match(r"v=spf1", v, re.IGNORECASE):
            return v
    return None


def _spf_posture(spf: str | None) -> str:
    if not spf:
        return "No SPF"
    m = re.search(r"([+\-~?]?)all(?:\s|$)", spf.lower())
    if not m:
        return "no ?all"
    q = m.group(1) or "+"
    return {"-": "-all", "~": "~all", "+": "+all", "?": "?all"}[q]


def _mx_provider(hostname: str) -> str:
    ext = tldextract.extract(hostname.rstrip("."))
    reg = ext.top_domain_under_public_suffix
    if reg and reg in MX_MAP:
        return MX_MAP[reg]["provider"]
    return reg or hostname.rstrip(".")


def _spf_includes(spf: str | None) -> list[dict]:
    if not spf:
        return []
    result = []
    for inc in re.findall(r"include:(\S+)", spf, re.IGNORECASE):
        entry = SPF_MAP.get(inc)
        if entry:
            result.append({
                "domain": inc,
                "provider": entry["provider"],
                "type": entry.get("type", ""),
            })
        else:
            result.append({"domain": inc, "provider": inc, "type": ""})
    return result


def _is_self_hosted(mx_vals: list[str] | None, reg_dom: str) -> bool:
    if not mx_vals:
        return False
    for h in mx_vals:
        ext = tldextract.extract(h.rstrip("."))
        if ext.top_domain_under_public_suffix == reg_dom:
            return True
    return False


# ---------------------------------------------------------------------------
# Segment builder
# ---------------------------------------------------------------------------

def _build_segments(events: list[tuple], reg_dom: str, cutoff: date) -> tuple[list, list]:
    """Return (mx_segments, spf_segments)."""
    from collections import defaultdict

    by_qt: dict[str, list] = defaultdict(list)
    for (_domain, qt, dt, val) in events:
        by_qt[qt].append((dt, val))

    # MX segments — deduplicate on value change (same as SPF)
    mx_segs: list[dict] = []
    prev_mx: list | None = "__unset__"
    for dt, val in by_qt.get("MX", []):
        if val == prev_mx:
            continue
        prev_mx = val
        if val:
            providers = list(dict.fromkeys(_mx_provider(h) for h in val))
            provider_str = " / ".join(providers)
            self_hosted = _is_self_hosted(val, reg_dom)
            mx_records = [h.rstrip(".") for h in val]
        else:
            provider_str = None
            self_hosted = False
            mx_records = []
        mx_segs.append({
            "start": dt,
            "end": None,
            "absent": val is None,
            "mx_records": mx_records,
            "provider": provider_str,
            "self_hosted": self_hosted,
        })
    for i, seg in enumerate(mx_segs):
        seg["end"] = mx_segs[i + 1]["start"] if i + 1 < len(mx_segs) else cutoff

    # SPF segments — deduplicate on SPF string change (TXT events fire for non-SPF TXT changes too)
    spf_segs: list[dict] = []
    prev_spf = "__unset__"
    for dt, val in by_qt.get("TXT", []):
        spf = _extract_spf(val)
        if spf == prev_spf:
            continue
        prev_spf = spf
        spf_segs.append({
            "start": dt,
            "end": None,
            "absent": spf is None,
            "spf": spf,
            "posture": _spf_posture(spf),
            "includes": _spf_includes(spf),
        })
    for i, seg in enumerate(spf_segs):
        seg["end"] = spf_segs[i + 1]["start"] if i + 1 < len(spf_segs) else cutoff

    return mx_segs, spf_segs


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_sources(domain: str) -> dict[str, bool]:
    """Return which sources contain data for this domain."""
    domain = domain.lower().strip().rstrip(".")
    ext = tldextract.extract(domain)
    reg_dom = ext.top_domain_under_public_suffix or domain
    bucket = _bucket(reg_dom)
    domain_fqdn = (domain + ".").replace("'", "''")
    where = f"domain = '{domain_fqdn}' AND query_type IN ('MX', 'TXT')"
    result = {}
    for name in SOURCES:
        globs = _parquet_globs(bucket, source=name)
        if not globs:
            result[name] = False
            continue
        con = duckdb.connect()
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{globs[0]}') WHERE {where}"
        ).fetchone()[0]
        result[name] = n > 0
    return result


def get_domain_history(domain: str, source: str) -> dict:
    """
    Fetch full MX and SPF segment history for an exact domain.
    source: 'toplists' or 'zonefiles'.
    Returns {domain, reg_dom, bucket, source, mx: [...], spf: [...], found: bool}.
    """
    domain = domain.lower().strip().rstrip(".")
    ext = tldextract.extract(domain)
    reg_dom = ext.top_domain_under_public_suffix or domain
    bucket = _bucket(reg_dom)
    globs = _parquet_globs(bucket, source=source)

    domain_fqdn = (domain + ".").replace("'", "''")
    events = (
        _fetch_events(globs[0], f"domain = '{domain_fqdn}' AND query_type IN ('MX', 'TXT')")
        if globs else []
    )
    cutoff = _dataset_cutoff(source)
    mx_segs, spf_segs = _build_segments(events, reg_dom, cutoff)
    return {
        "domain": domain,
        "reg_dom": reg_dom,
        "bucket": bucket,
        "source": source,
        "mx": mx_segs,
        "spf": spf_segs,
        "found": bool(events),
        "cutoff": cutoff,
    }


def get_dataset_metrics() -> dict:
    """Combined metrics across all sources: historical unique domains, event count, date range."""
    con = duckdb.connect()

    # Unique domains — count distinct across all events (historical, not just current state).
    # Expensive but cached; single pass over all buckets via glob.
    event_globs_for_domains = []
    for source_dir in SOURCES.values():
        if any(source_dir.glob("bucket=*/*.parquet")):
            event_globs_for_domains.append(str(source_dir / "bucket=*" / "*.parquet"))

    domain_count = 0
    if event_globs_for_domains:
        file_list = "[" + ", ".join(f"'{g}'" for g in event_globs_for_domains) + "]"
        domain_count = con.execute(
            f"SELECT COUNT(DISTINCT domain) FROM read_parquet({file_list}, hive_partitioning=false)"
        ).fetchone()[0]

    # Event count + date range — DuckDB uses Parquet footer stats for COUNT(*) and MIN/MAX.
    event_count = 0
    min_date = None
    max_date = None
    if event_globs_for_domains:
        file_list = "[" + ", ".join(f"'{g}'" for g in event_globs_for_domains) + "]"
        row = con.execute(
            f"SELECT COUNT(*), MIN(measurement_date), MAX(measurement_date) "
            f"FROM read_parquet({file_list}, hive_partitioning=false)"
        ).fetchone()
        event_count, min_date, max_date = row

    return {
        "domain_count": domain_count,
        "event_count": event_count,
        "min_date": min_date,
        "max_date": max_date,
    }


def get_mx_provider_distribution(top_n: int = 15) -> tuple[list[dict], int]:
    """
    Current MX provider market share across all sources.
    Returns (rows, total_mx_domains) where rows are sorted by domain_count desc.
    Percentages are relative to total_mx_domains.
    """
    from collections import defaultdict

    state_files = [
        str(REPO / "data" / s / "state.parquet")
        for s in SOURCES
        if (REPO / "data" / s / "state.parquet").exists()
    ]
    if not state_files:
        return [], 0

    file_list = "[" + ", ".join(f"'{f}'" for f in state_files) + "]"
    con = duckdb.connect()

    # Per-domain approx registrable domain: extract last 2 dot-separated parts from each MX host,
    # deduplicate (domain, approx_reg) so multi-host domains aren't overcounted per provider.
    rows = con.execute(
        f"WITH u AS ("
        f"  SELECT DISTINCT domain, unnest(value) AS h"
        f"  FROM read_parquet({file_list})"
        f"  WHERE query_type='MX' AND value IS NOT NULL"
        f"), rd AS ("
        f"  SELECT domain, regexp_extract(lower(trim(h, '.')), '([^.]+[.][^.]+)$') AS approx_reg"
        f"  FROM u"
        f")"
        f"SELECT approx_reg, COUNT(DISTINCT domain) AS cnt"
        f" FROM rd WHERE approx_reg != ''"
        f" GROUP BY approx_reg ORDER BY cnt DESC"
    ).fetchall()

    total_mx = con.execute(
        f"SELECT COUNT(DISTINCT domain) FROM read_parquet({file_list})"
        f" WHERE query_type='MX' AND value IS NOT NULL"
    ).fetchone()[0]

    provider_counts: dict[str, int] = defaultdict(int)
    for approx_reg, cnt in rows:
        entry = MX_MAP.get(approx_reg)
        provider = entry["provider"] if entry else approx_reg
        provider_counts[provider] += cnt

    sorted_providers = sorted(provider_counts.items(), key=lambda x: -x[1])[:top_n]
    result = [
        {
            "provider": p,
            "domain_count": c,
            "percentage": round(100 * c / total_mx, 1) if total_mx else 0,
        }
        for p, c in sorted_providers
    ]
    return result, total_mx


def get_spf_tool_distribution(top_n: int = 15) -> tuple[list[dict], int]:
    """
    Current SPF include provider adoption across all sources.
    Returns (rows, total_spf_domains) where rows are sorted by domain_count desc.
    Percentages are relative to total_spf_domains (domains with a valid SPF record).
    """
    from collections import defaultdict

    state_files = [
        str(REPO / "data" / s / "state.parquet")
        for s in SOURCES
        if (REPO / "data" / s / "state.parquet").exists()
    ]
    if not state_files:
        return [], 0

    file_list = "[" + ", ".join(f"'{f}'" for f in state_files) + "]"
    con = duckdb.connect()

    total_spf = con.execute(
        f"SELECT COUNT(DISTINCT domain) FROM read_parquet({file_list})"
        f" WHERE query_type='TXT' AND value IS NOT NULL"
        f" AND array_to_string(value, ' ') ILIKE '%v=spf1%'"
    ).fetchone()[0]

    rows = con.execute(
        f"WITH spf AS ("
        f"  SELECT DISTINCT domain, array_to_string(value, ' ') AS spf_str"
        f"  FROM read_parquet({file_list})"
        f"  WHERE query_type='TXT' AND value IS NOT NULL"
        f"    AND array_to_string(value, ' ') ILIKE '%v=spf1%'"
        f"), inc AS ("
        f"  SELECT domain, unnest(regexp_extract_all(spf_str, 'include:([^ ]+)', 1)) AS inc_domain"
        f"  FROM spf"
        f")"
        f"SELECT lower(inc_domain) AS inc, COUNT(DISTINCT domain) AS cnt"
        f" FROM inc WHERE inc_domain != ''"
        f" GROUP BY lower(inc_domain) ORDER BY cnt DESC"
    ).fetchall()

    provider_counts: dict[str, int] = defaultdict(int)
    provider_meta: dict[str, dict] = {}
    for inc_domain, cnt in rows:
        entry = SPF_MAP.get(inc_domain)
        if entry:
            name = entry["provider"]
            ptype = entry.get("type", "")
        else:
            name = inc_domain
            ptype = ""
        provider_counts[name] += cnt
        if name not in provider_meta:
            provider_meta[name] = {"type": ptype}

    sorted_providers = sorted(provider_counts.items(), key=lambda x: -x[1])[:top_n]
    result = [
        {
            "provider": p,
            "type": provider_meta.get(p, {}).get("type", ""),
            "domain_count": c,
            "percentage": round(100 * c / total_spf, 1) if total_spf else 0,
        }
        for p, c in sorted_providers
    ]
    return result, total_spf


def get_suffix_domains(suffix: str, limit: int = 300) -> list[str]:
    """
    Return distinct domains whose hostname ends with suffix, ordered by event count desc.
    Works for any suffix (registrable domain or deeper, e.g. mail.utwente.nl).
    """
    suffix = suffix.lower().strip().rstrip(".")
    ext = tldextract.extract(suffix)
    reg_dom = ext.top_domain_under_public_suffix or suffix
    bucket = _bucket(reg_dom)
    globs = _parquet_globs(bucket)
    if not globs:
        return []
    con = duckdb.connect()
    # domain_reversed has a leading dot (FQDN trailing dot reversed):
    # "mail.utwente.nl." → ".nl.utwente.mail"
    rev_prefix = "." + ".".join(reversed(suffix.split(".")))
    rev_sql = rev_prefix.replace("'", "''")
    parts = [
        f"SELECT domain FROM read_parquet('{g}') "
        f"WHERE (domain_reversed = '{rev_sql}' OR domain_reversed LIKE '{rev_sql}.%') "
        f"AND query_type IN ('MX', 'TXT')"
        for g in globs
    ]
    sql = (
        f"SELECT domain, count(*) c FROM ({' UNION ALL '.join(parts)}) "
        f"GROUP BY domain ORDER BY c DESC LIMIT {limit}"
    )
    rows = con.execute(sql).fetchall()
    return [r[0].rstrip(".") for r in rows]
