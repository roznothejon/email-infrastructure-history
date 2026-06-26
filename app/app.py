"""Streamlit app: email infrastructure history explorer."""
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.colors as pc
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parent))
from queries import SOURCES, check_sources, get_domain_history, get_suffix_domains

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
POSTURE_COLOR = {
    "-all": "#27ae60",   # green  — strict reject
    "~all": "#e67e22",   # orange — soft fail
    "+all": "#e74c3c",   # red    — accept everything (bad)
    "?all": "#f1c40f",   # yellow — neutral
    "no ?all": "#95a5a6",
    "No SPF": "#d5d8dc",
}
ABSENT_COLOR = "#45475a"

POSTURE_EXPLAIN = {
    "-all": (
        "Strict reject",
        "Only explicitly listed senders are authorized. All others are **rejected**. "
        "Strong protection against spoofing.",
    ),
    "~all": (
        "Soft fail",
        "Unlisted senders are flagged as suspicious but **not rejected** — enforcement depends on the recipient's policy. "
        "Common during migrations.",
    ),
    "+all": (
        "Permissive (insecure)",
        "**All senders** are authorized, including untrusted ones. SPF provides no protection against spoofing.",
    ),
    "?all": (
        "Neutral",
        "No enforcement policy. Unlisted senders are treated neutrally — effectively a no-op.",
    ),
    "no ?all": (
        "No catch-all",
        "SPF record has no `all` mechanism. Behaviour for unlisted senders is undefined.",
    ),
    "No SPF": (
        "No SPF record",
        "No SPF record found. Anyone can forge mail from this domain without SPF-level protection.",
    ),
}

TYPE_LABEL: dict[str, str] = {
    "marketing": "marketing",
    "security": "security / filtering",
    "hosting": "hosting",
    "transactional": "transactional email",
    "email_provider": "email provider",
    "crm": "CRM",
    "erp": "ERP",
    "helpdesk": "helpdesk",
    "ecommerce": "e-commerce",
    "finance": "finance",
    "signature": "email signatures",
    "other": "other",
    "": "third-party service",
}
PROV_PALETTE = pc.qualitative.Plotly + pc.qualitative.D3 + pc.qualitative.G10


def _prov_color(name: str) -> str:
    return PROV_PALETTE[abs(hash(name)) % len(PROV_PALETTE)]


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------

def _dur_ms(start: date, end: date) -> int:
    return int((pd.Timestamp(end) - pd.Timestamp(start)).total_seconds() * 1000)


def _hover_mx(seg: dict) -> str:
    lines = []
    if seg["absent"]:
        lines.append("<b>No MX record</b>")
    else:
        lines.append(f"<b>{seg['provider']}</b>")
        for r in seg["mx_records"]:
            lines.append(f"&nbsp;&nbsp;{r}")
        if seg["self_hosted"]:
            lines.append("Self-hosted: Yes")
    lines.append(f"<i>{seg['start']} → {seg['end']}</i>")
    return "<br>".join(lines)


def _hover_spf(seg: dict) -> str:
    lines = []
    if seg["absent"]:
        lines.append("<b>No SPF record</b>")
    else:
        lines.append(f"<b>Posture: {seg['posture']}</b>")
        if seg["includes"]:
            lines.append("Includes:")
            for inc in seg["includes"]:
                t = f" ({inc['type']})" if inc["type"] else ""
                lines.append(f"&nbsp;&nbsp;• {inc['provider']}{t}")
        if seg["spf"]:
            raw = seg["spf"]
            if len(raw) > 120:
                raw = raw[:120] + "…"
            lines.append(f"<i>{raw}</i>")
    lines.append(f"<i>{seg['start']} → {seg['end']}</i>")
    return "<br>".join(lines)


def build_figure(result: dict) -> go.Figure | None:
    has_mx = bool(result["mx"])
    has_spf = bool(result["spf"])
    if not has_mx and not has_spf:
        return None

    rows_n = (1 if has_mx else 0) + (1 if has_spf else 0)
    titles = (["MX Records"] if has_mx else []) + (["SPF Records"] if has_spf else [])
    heights = [0.45, 0.55] if rows_n == 2 else [1.0]

    fig = make_subplots(
        rows=rows_n,
        cols=1,
        shared_xaxes=True,
        subplot_titles=titles,
        row_heights=heights,
        vertical_spacing=0.12,
    )

    legend_shown: set[str] = set()
    mx_row = 1
    spf_row = 2 if has_mx else 1

    def _add_bar(row: int, y_label: str, seg: dict, label: str, color: str, hover: str) -> None:
        show = label not in legend_shown
        legend_shown.add(label)
        fig.add_trace(
            go.Bar(
                x=[_dur_ms(seg["start"], seg["end"])],
                y=[y_label],
                base=[seg["start"].isoformat()],
                orientation="h",
                name=label,
                marker_color=color,
                marker_line_width=0.5,
                marker_line_color="#1e1e2e",
                showlegend=show,
                legendgroup=label,
                customdata=[[hover]],
                hovertemplate="%{customdata[0]}<extra></extra>",
            ),
            row=row,
            col=1,
        )

    # MX
    if has_mx:
        for seg in result["mx"]:
            label = "No MX" if seg["absent"] else (seg["provider"] or "Unknown")
            color = ABSENT_COLOR if seg["absent"] else _prov_color(label)
            _add_bar(mx_row, "MX", seg, label, color, _hover_mx(seg))

    # SPF
    if has_spf:
        for seg in result["spf"]:
            label = "No SPF" if seg["absent"] else seg["posture"]
            color = POSTURE_COLOR.get(label, "#aaa")
            _add_bar(spf_row, "SPF", seg, label, color, _hover_spf(seg))

    #dark mode colors
    _dark = "#1e1e2e"
    _grid = "#2e2e3e"
    _text = "#cdd6f4"
    _subtext = "#7f849c"

    #light mode colors
    # _dark = "#ffffff"
    # _grid = "#e0e0e0"
    # _text = "#333333"
    # _subtext = "#666666"


    fig.update_xaxes(
        type="date",
        gridcolor=_grid,
        linecolor=_grid,
        tickfont=dict(color=_subtext),
    )
    fig.update_yaxes(
        gridcolor=_grid,
        linecolor=_grid,
        tickfont=dict(color=_subtext),
        ticklen=0,
    )
    # Subplot title colour
    for ann in fig.layout.annotations:
        ann.font.color = _text
        ann.font.size = 11

    fig.update_layout(
        barmode="overlay",
        height=280 if rows_n == 2 else 160,
        title=dict(text=f"<b>{result['domain']}</b>", x=0.5, font_size=14, font_color=_text),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.08, x=0,
            font=dict(size=11, color=_text),
            bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=55, r=15, t=90, b=30),
        plot_bgcolor=_dark,
        paper_bgcolor=_dark,
        hoverlabel=dict(bgcolor="#313244", font_color=_text, bordercolor=_grid),
    )
    return fig


# ---------------------------------------------------------------------------
# Streamlit caching wrappers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def cached_domain_history(domain: str, source: str) -> dict:
    return get_domain_history(domain, source=source)


@st.cache_data(ttl=3600)
def cached_suffix_domains(suffix: str) -> list[str]:
    return get_suffix_domains(suffix)


@st.cache_data(ttl=3600)
def cached_check_sources(domain: str) -> dict[str, bool]:
    return check_sources(domain)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Email Infrastructure History",
    page_icon="✉",
    layout="wide",
)
st.markdown(
    '<style>[data-testid="stAppDeployButton"]{display:none}</style>',
    unsafe_allow_html=True,
)

st.title("Email Infrastructure History")
st.caption(
    "Track how a domain's MX and SPF records have changed over time. "
    "Use `%suffix` to explore all domains under a registrable domain (e.g. `%utwente.nl`)."
)

query = st.text_input(
    "Domain or suffix",
    placeholder="utwente.nl  or  %utwente.nl",
    label_visibility="collapsed",
)

if not query:
    st.info("Enter a domain name or a `%suffix` pattern to explore.")
    st.stop()

query = query.strip().lower()
target: str

if query.startswith("%"):
    suffix = query.lstrip("%").strip()
    if not suffix:
        st.error("Enter a valid suffix after `%`.")
        st.stop()

    with st.spinner(f"Looking up domains under `{suffix}`…"):
        domains = cached_suffix_domains(suffix)

    if not domains:
        st.warning(f"No MX or SPF history found under `{suffix}`.")
        st.stop()

    st.caption(f"**{len(domains)}** domains with email history under `{suffix}`")

    filter_text = st.text_input(
        "Filter", placeholder="Type to filter domain list…", label_visibility="collapsed"
    )
    filtered = (
        [d for d in domains if filter_text.lower() in d] if filter_text else domains
    )
    if not filtered:
        st.warning("No domains match the filter.")
        st.stop()

    target = st.selectbox("Select domain", filtered, label_visibility="collapsed")
else:
    target = query

available = cached_check_sources(target)
present_in = [name for name, ok in available.items() if ok]

def _source_label(key: str) -> str:
    return key.replace("_", " ").replace("-", " ").title()


source_filter: str | None = None
if len(present_in) == 1:
    source_filter = present_in[0]
elif len(present_in) > 1:
    ordered = [k for k in SOURCES if k in present_in]
    source_filter = st.radio(
        "Dataset",
        ordered,
        format_func=_source_label,
        horizontal=True,
        label_visibility="collapsed",
    )

with st.spinner(f"Loading history for `{target}`…"):
    result = cached_domain_history(target, source=source_filter)

if not result["found"]:
    st.warning(f"No MX or SPF history found for `{target}`.")
    st.stop()

# Metrics row
mx_changes = len(result["mx"])
spf_changes = len(result["spf"])
all_segs = result["mx"] + result["spf"]
first_date = min((s["start"] for s in all_segs), default=None)
last_date = max(
    (s["start"] for s in all_segs if not s["absent"]), default=None
)

source_label = _source_label(result["source"])

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("MX changes", mx_changes)
c2.metric("SPF changes", spf_changes)
if first_date:
    c3.metric("First seen", str(first_date))
if last_date:
    c4.metric("Last change", str(last_date))
c5.metric("Dataset", source_label)
c6.metric("Data through", str(result["cutoff"]))

# Timeline
fig = build_figure(result)
if fig:
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No chart data available.")

# Current snapshot summary
cur_mx = result["mx"][-1] if result["mx"] else None
cur_spf = result["spf"][-1] if result["spf"] else None

st.divider()
col_mx, col_spf, col_partners = st.columns(3)

with col_mx:
    st.markdown("#### Mail hosting", help="Who is handling their email hosting?")
    if cur_mx is None:
        st.caption("No MX history.")
    elif cur_mx["absent"]:
        st.info("No MX record currently.")
    else:
        provider = cur_mx["provider"] or "Unknown"
        st.markdown(f"**{provider}**")
        if cur_mx["self_hosted"]:
            st.caption("Self-hosted (MX points back to own domain)")
        for rec in cur_mx["mx_records"]:
            st.caption(f"`{rec}`")

with col_spf:
    st.markdown("#### SPF security posture", help="What is their SPF security posture?")
    if cur_spf is None:
        st.caption("No SPF history.")
    elif cur_spf["absent"]:
        title, explain = POSTURE_EXPLAIN["No SPF"]
        st.warning(f"**{title}**")
        st.caption(explain)
    else:
        posture = cur_spf["posture"]
        title, explain = POSTURE_EXPLAIN.get(posture, (posture, ""))
        if posture == "-all":
            st.success(f"**{title}** (`{posture}`)")
        elif posture in ("~all", "no ?all"):
            st.warning(f"**{title}** (`{posture}`)")
        elif posture == "+all":
            st.error(f"**{title}** (`{posture}`)")
        else:
            st.info(f"**{title}** (`{posture}`)")
        st.caption(explain)

with col_partners:
    st.markdown("#### SPF partnerships & senders", help="Which third-party services are authorized to send email on their behalf and for what purposes?")
    if cur_spf is None or cur_spf["absent"]:
        st.caption("No SPF record.")
    else:
        includes = cur_spf.get("includes", [])
        if includes:
            for inc in includes:
                type_str = TYPE_LABEL.get(inc.get("type", ""), "third-party service")
                st.markdown(f"- **{inc['provider']}** — {type_str}")
        else:
            st.caption("No `include:` directives.")
        spf_raw = cur_spf.get("spf") or ""
        raw_ips = re.findall(r"ip[46]:([^\s]+)", spf_raw, re.IGNORECASE)
        if raw_ips:
            st.markdown("**Raw IP authorizations:**", help="This record authorizes specific IP addresses directly. This could mean they are self-managing (a part of) their email infrastructure, or it could be a third-party provider that doesn't use `include:`.")
            for ip in raw_ips:
                st.caption(f"`{ip}`")

st.divider()

# Detail tables
with st.expander("MX record history", expanded=False):
    if result["mx"]:
        rows = [
            {
                "From": str(s["start"]),
                "To": str(s["end"]),
                "Provider": s["provider"] or "—",
                "MX records": ", ".join(s["mx_records"]) if s["mx_records"] else "—",
                "Self-hosted": "Yes" if s["self_hosted"] else "No",
                "Absent": "Yes" if s["absent"] else "No",
            }
            for s in result["mx"]
        ]
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.write("No MX history.")

with st.expander("SPF record history", expanded=False):
    if result["spf"]:
        rows = [
            {
                "From": str(s["start"]),
                "To": str(s["end"]),
                "Posture": s["posture"],
                "Includes": ", ".join(i["provider"] for i in s["includes"]) or "—",
                "Full SPF": s["spf"] or "—",
                "Absent": "Yes" if s["absent"] else "No",
            }
            for s in result["spf"]
        ]
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.write("No SPF history.")
