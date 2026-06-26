"""About / FAQ / Metrics page."""
import sys
from pathlib import Path

import pandas as pd
import plotly.colors as pc
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from queries import get_dataset_metrics, get_mx_provider_distribution, get_spf_tool_distribution

st.set_page_config(
    page_title="About — Email Infrastructure History",
    page_icon="✉",
    layout="wide",
)
st.markdown(
    '<style>[data-testid="stAppDeployButton"]{display:none}</style>',
    unsafe_allow_html=True,
)

st.title("About this tool")
st.caption("Dataset info, live metrics and other mildly interesting things")

st.markdown(
    "This tool visualizes the email infrastructure history of domains tracked by OpenINTEL, "
    "a large-scale DNS measurement project at the University of Twente. For each domain, it shows "
    "how MX records (which determine who handles incoming email) and SPF records (which control who "
    "is authorized to send email on the domain's behalf) have changed over time - letting you trace "
    "migrations between providers like Google Workspace and Microsoft 365, spot when a domain added "
    "a marketing or CRM tool, and assess their current email security posture."
)

st.divider()

# ---------------------------------------------------------------------------
# Dataset overview
# ---------------------------------------------------------------------------
st.header("Dataset")

@st.cache_data(ttl=86400)
def _metrics() -> dict:
    return get_dataset_metrics()


with st.spinner("Loading dataset metrics…"):
    m = _metrics()

def _fmt_int(n: int) -> str:
    return f"{n:,}"

def _fmt_date_range(mn, mx) -> str:
    if mn is None or mx is None:
        return "—"
    return f"{mn} → {mx}"

col_a, col_b, col_c = st.columns(3)
with col_a:
    st.metric(
        "Domains tracked",
        _fmt_int(m["domain_count"]) if m["domain_count"] else "—",
        help="Unique domains with at least one MX or SPF event (combined across all sources).",
    )
with col_b:
    st.metric(
        "Events recorded",
        _fmt_int(m["event_count"]) if m["event_count"] else "—",
        help="Total event rows across all sources and buckets.",
    )
with col_c:
    st.metric(
        "Date range",
        _fmt_date_range(m["min_date"], m["max_date"]),
        help="Earliest and latest measurement dates in the event store.",
    )

st.divider()

# ---------------------------------------------------------------------------
# Provider landscape
# ---------------------------------------------------------------------------
st.header("Provider landscape")
st.caption("Current snapshot — domains with active MX / SPF records, all sources combined.")

# dark mode colors
_DARK = "#1e1e2e"
_GRID = "#2e2e3e"
_TEXT = "#cdd6f4"
_SUBTEXT = "#7f849c"


#light mode colors
# _DARK = "#ffffff"
# _GRID = "#e0e0e0"
# _TEXT = "#333333"
# _SUBTEXT = "#666666"


def _hbar(labels: list[str], values: list[float], text: list[str], color: str) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=values,
        y=labels,
        orientation="h",
        text=text,
        textposition="outside",
        marker_color=color,
        marker_line_width=0,
        cliponaxis=False,
    ))
    fig.update_layout(
        height=max(220, 36 * len(labels) + 60),
        margin=dict(l=10, r=80, t=10, b=30),
        plot_bgcolor=_DARK,
        paper_bgcolor=_DARK,
        xaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            tickfont=dict(color=_SUBTEXT),
            ticksuffix="%",
            range=[0, max(values) * 1.18] if values else [0, 1],
        ),
        yaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            tickfont=dict(color=_TEXT),
            autorange="reversed",
        ),
        font=dict(color=_TEXT),
    )
    return fig


@st.cache_data(ttl=86400)
def _mx_dist() -> tuple[list[dict], int]:
    return get_mx_provider_distribution(top_n=10)


@st.cache_data(ttl=86400)
def _spf_dist() -> tuple[list[dict], int]:
    return get_spf_tool_distribution(top_n=10)


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
    "": "third-party",
}

col_mx, col_spf = st.columns(2)

with col_mx:
    st.subheader("Email hosting (MX)")
    with st.spinner("Loading…"):
        mx_rows, mx_total = _mx_dist()
    if mx_rows:
        st.caption(f"{mx_total:,} domains with active MX records")
        labels = [r["provider"] for r in mx_rows]
        pcts = [r["percentage"] for r in mx_rows]
        texts = [f"{r['percentage']}% ({r['domain_count']:,})" for r in mx_rows]
        st.plotly_chart(_hbar(labels, pcts, texts, "#4c9be8"), use_container_width=True)
    else:
        st.info("No data.")

with col_spf:
    st.subheader("SPF third-party senders")
    with st.spinner("Loading…"):
        spf_rows, spf_total = _spf_dist()
    if spf_rows:
        st.caption(f"{spf_total:,} domains with SPF records")
        labels = [r["provider"] for r in spf_rows]
        pcts = [r["percentage"] for r in spf_rows]
        texts = [
            f"{r['percentage']}% ({r['domain_count']:,})"
            + (f" — {TYPE_LABEL.get(r['type'], r['type'])}" if r["type"] else "")
            for r in spf_rows
        ]
        st.plotly_chart(_hbar(labels, pcts, texts, "#a6e3a1"), use_container_width=True)
    else:
        st.info("No data.")

st.divider()

# ---------------------------------------------------------------------------
# Adoption trends
# ---------------------------------------------------------------------------
st.header("Adoption trends")
st.caption(
    "Monthly share of SPF-using domains that include each category or service — "
    "computed from the top list event store."
)
# Run `scripts/analysis/spf_adoption_monthly.py --source toplists --output-dir data/stats/spf_adoption/toplists` to refresh.
# Zonefiles SPF data is unreliable, so adoption trends are sourced from toplists only.
_STATS = Path(__file__).resolve().parent.parent.parent / "data" / "stats" / "spf_adoption" / "toplists"


@st.cache_data(ttl=86400)
def _load_adoption() -> "tuple[pd.DataFrame | None, pd.DataFrame | None]":
    cat_p = _STATS / "categories_monthly.parquet"
    inc_p = _STATS / "includes_monthly.parquet"
    if not cat_p.exists() or not inc_p.exists():
        return None, None
    return pd.read_parquet(cat_p), pd.read_parquet(inc_p)


def _line_layout(height: int = 400) -> dict:
    return dict(
        height=height,
        margin=dict(l=10, r=10, t=10, b=30),
        plot_bgcolor=_DARK,
        paper_bgcolor=_DARK,
        xaxis=dict(gridcolor=_GRID, linecolor=_GRID, tickfont=dict(color=_SUBTEXT)),
        yaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            tickfont=dict(color=_SUBTEXT),
            ticksuffix="%",
            title=dict(text="% of SPF domains", font=dict(color=_SUBTEXT)),
        ),
        font=dict(color=_TEXT),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=_TEXT, size=11),
        ),
        hovermode="x unified",
    )


with st.spinner("Loading adoption data…"):
    _cat_df, _inc_df = _load_adoption()

if _cat_df is None:
    st.info(
        "Adoption data not found. "
        "Run `python scripts/analysis/spf_adoption_monthly.py` to generate it."
    )
else:
    # ---- Chart 1: categories ----
    st.subheader("By category")
    _cat_colors = pc.qualitative.Plotly
    _cat_fig = go.Figure()
    for i, cat in enumerate(_cat_df.groupby("category")["pct"].max().sort_values(ascending=False).index):
        sub = _cat_df[_cat_df["category"] == cat].sort_values("month")
        _cat_fig.add_trace(go.Scatter(
            x=sub["month"],
            y=sub["pct"],
            name=TYPE_LABEL.get(cat, cat),
            mode="lines",
            line=dict(color=_cat_colors[i % len(_cat_colors)], width=2),
            customdata=sub[["domain_count", "total_spf_domains"]].values,
            hovertemplate=(
                f"<b>{TYPE_LABEL.get(cat, cat)}</b><br>"
                "%{y:.1f}% (%{customdata[0]:,} of %{customdata[1]:,} domains)<extra></extra>"
            ),
        ))
    _cat_fig.update_layout(**_line_layout())
    st.plotly_chart(_cat_fig, use_container_width=True)

    # ---- Chart 2: top includes ----
    st.subheader("By service (top 20)")
    st.caption("Top 20 SPF include domains by share in the most recent month.")
    _latest = _inc_df["month"].max()
    _top20 = (
        _inc_df[_inc_df["month"] == _latest]
        .nlargest(20, "pct")["inc_domain"]
        .tolist()
    )
    _inc_sub = _inc_df[_inc_df["inc_domain"].isin(_top20)]
    _inc_colors = pc.qualitative.Dark24
    _inc_fig = go.Figure()
    for i, inc in enumerate(_top20):
        sub = _inc_sub[_inc_sub["inc_domain"] == inc].sort_values("month")
        if sub.empty:
            continue
        provider = sub["provider"].iloc[0]
        _inc_fig.add_trace(go.Scatter(
            x=sub["month"],
            y=sub["pct"],
            name=provider,
            mode="lines",
            line=dict(color=_inc_colors[i % len(_inc_colors)], width=2),
            customdata=sub[["domain_count", "total_spf_domains", "inc_domain"]].values,
            hovertemplate=(
                f"<b>{provider}</b> ({inc})<br>"
                "%{y:.1f}% (%{customdata[0]:,} of %{customdata[1]:,} domains)<extra></extra>"
            ),
        ))
    _inc_fig.update_layout(**_line_layout())
    st.plotly_chart(_inc_fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Data sources
# ---------------------------------------------------------------------------
st.header("Data sources")

st.markdown("### Top lists")
st.markdown(
    "Union of the top 1M domains from Umbrella, Tranco, Radar, Alexa and Majestic. "
    "Together they cover the most popular domains, but the tail end of the distribution "
    "is very volatile — coverage may not be consistent."
)

st.markdown("### Zone files")
st.markdown(
    "Measurements taken directly from the .ee, .fr, .gov, .li, .nu, .se and .sk zone files. "
    "They cover all domains under those ccTLDs. Coverage is consistent, "
    "but the domains are generally less popular."
)

st.divider()

# ---------------------------------------------------------------------------
# FAQ
# ---------------------------------------------------------------------------
st.header("Q")

# with st.expander("What is this tool?"):
#     st.markdown(
#         "This tool visualizes the email infrastructure history of domains tracked by OpenINTEL, "
#         "a large-scale DNS measurement project at the University of Twente. For each domain, it shows "
#         "how MX records (which determine who handles incoming email) and SPF records (which control who "
#         "is authorized to send email on the domain's behalf) have changed over time — letting you trace "
#         "migrations between providers like Google Workspace and Microsoft 365, spot when a domain added "
#         "a marketing or CRM tool, and assess their current email security posture."
#     )

with st.expander("What is SPF and why does it matter?"):
    st.markdown(
        "SPF (Sender Policy Framework) is an email authentication mechanism that allows domain owners "
        "to specify which mail servers are authorized to send email on their behalf. It is published as "
        "a DNS TXT record. SPF helps prevent email spoofing and phishing by enabling receiving mail "
        "servers to verify that incoming emails claiming to be from a domain are sent from authorized "
        "sources. In other words, SPF records show who is authorized to send emails on behalf of a "
        "domain. This information can be used to infer what third parties a domain is partnering with, "
        "and for what purposes."
    )

with st.expander("What is an MX record?"):
    st.markdown(
        "MX (Mail Exchange) records are DNS records that specify the mail servers responsible for "
        "receiving email on behalf of a domain. They indicate where emails sent to addresses at that "
        "domain should be delivered. Initially, mail exchange servers were mostly self-hosted, but "
        "nowadays this is mostly outsourced to a number of large providers. So MX records can show us "
        "what company is responsible for handling a domain's email."
    )

with st.expander("How are email providers identified?"):
    st.markdown(
        "Lookup tables mapping MX hostnames and SPF includes were built using AI. The most popular "
        "current and historical records were extracted to build these tables. Since they came out of "
        "an LLM, they should be taken with a grain of salt, although manual checks suggest "
        "precautions have been taken against hallucinations, and they should be mostly accurate."
    )

with st.expander("What does 'self-hosted' mean?"):
    st.markdown(
        "Self-hosted means that an entity is managing their own email infrastructure. If a domain's "
        "MX record has the same registrable domain as the domain itself, it's marked as being "
        "self-hosted."
    )

with st.expander("Why might a domain be missing?"):
    st.markdown(
        "Multiple reasons: top list volatility, OpenINTEL worker outages, DNS outages, or outages "
        "somewhere else along the way. But it's most likely toplist volatility."
    )

st.divider()

# ---------------------------------------------------------------------------
# About the research
# ---------------------------------------------------------------------------
st.header("About the research")
st.markdown("This tool is a part of a bachelor's graduation project at the University of Twente named \"Life of a domain name: episode 2 - email\"."
            "The aim of this project is to convert the daily DNS snapshots taken by the OpenINTEL project into a format that allows for easy querying"
            "and visualization of the email infrastructure history of domains, as well as to create a user-friendly web-based tool to visualize them.")

import streamlit as st

st.header("Links")
st.markdown("""
- [OpenINTEL project](https://www.openintel.nl/)
- [Source code](https://github.com/roznothejon/email-infrastructure-history)
- [Paper](link)
""")


