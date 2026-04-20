"""
Mapping the U.S. Digital Economy
Interactive Dashboard — Streamlit + Plotly
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import numpy as np

# ── Config ───────────────────────────────────────────────────────────────────
DB_FILE = Path(__file__).parent / "data" / "companies.db"

st.set_page_config(
    page_title="Mapping the U.S. Digital Economy",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# State name → abbreviation mapping
STATE_ABBREV = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
}

# ── Styles ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .insight-box {
    background: #f0f4ff;
    border-left: 4px solid #4361ee;
    padding: 14px 18px;
    border-radius: 4px;
    margin: 12px 0;
  }
  .insight-box b { color: #1a237e; }
  .takeaway {
    background: #e8f5e9;
    border-left: 4px solid #2e7d32;
    padding: 10px 16px;
    border-radius: 4px;
    font-weight: 600;
    margin: 8px 0;
  }
  .case-card {
    background: #fff8e1;
    border: 1px solid #f9a825;
    border-radius: 8px;
    padding: 18px;
    margin: 8px 0;
  }
  .limit-box {
    background: #fff3e0;
    border-left: 4px solid #e65100;
    padding: 12px 16px;
    border-radius: 4px;
    margin: 8px 0;
    font-size: 0.9em;
  }
  .big-stat {
    font-size: 2.6em;
    font-weight: 900;
    color: #1a237e;
    line-height: 1.1;
    letter-spacing: -0.02em;
  }
  .big-stat-label {
    font-size: 0.85em;
    color: #666;
    font-weight: 400;
    display: block;
    margin-top: 2px;
  }
  .stat-card {
    background: #f5f7ff;
    border: 1px solid #c5cae9;
    border-top: 4px solid #3949ab;
    border-radius: 8px;
    padding: 18px 20px;
    text-align: center;
  }

  /* Tab navigation — full width, large */
  .stTabs [data-baseweb="tab-list"] {
    gap: 0px;
    border-bottom: 3px solid #e0e0e0;
    width: 100%;
    display: flex;
  }
  .stTabs [data-baseweb="tab"] {
    font-size: 1.15em !important;
    font-weight: 700 !important;
    padding: 18px 0 !important;
    flex: 1 !important;
    text-align: center !important;
    justify-content: center !important;
    color: #666 !important;
    background: #fafafa !important;
    border-right: 1px solid #e0e0e0 !important;
  }
  .stTabs [data-baseweb="tab"]:last-child {
    border-right: none !important;
  }
  .stTabs [aria-selected="true"] {
    color: #1a237e !important;
    background: #f0f4ff !important;
    border-bottom: 4px solid #1a237e !important;
  }
  .stTabs [data-baseweb="tab"]:hover {
    color: #1a237e !important;
    background: #e8eeff !important;
  }
</style>
""", unsafe_allow_html=True)


# ── Drill-down helper ────────────────────────────────────────────────────────
def show_sector_drilldown(sector_name: str, companies_df, company_revenue_df):
    """Show company list for a clicked sector."""
    filtered = companies_df[companies_df["sector"] == sector_name].copy()
    if filtered.empty:
        st.info(f"No companies found for **{sector_name}**.")
        return

    # Merge latest revenue
    filtered = filtered.merge(company_revenue_df, left_on="id", right_on="company_id", how="left")
    filtered["revenue_display"] = filtered["revenue"].apply(
        lambda x: f"${x/1e9:.2f}B" if pd.notna(x) and x >= 1e9
        else (f"${x/1e6:.0f}M" if pd.notna(x) else "—")
    )
    filtered["rd_display"] = filtered["rd_expense"].apply(
        lambda x: f"${x/1e9:.2f}B" if pd.notna(x) and x >= 1e9
        else (f"${x/1e6:.0f}M" if pd.notna(x) else "—")
    )
    filtered["year_display"] = filtered["year"].apply(lambda x: str(int(x)) if pd.notna(x) else "—")

    display = filtered[[
        "name", "hub_label", "state", "company_size",
        "revenue_model", "revenue_display", "rd_display", "year_display"
    ]].rename(columns={
        "name": "Company", "hub_label": "Hub", "state": "State",
        "company_size": "Size", "revenue_model": "Revenue Model",
        "revenue_display": "Revenue (latest)", "rd_display": "R&D Expense",
        "year_display": "Data Year",
    }).sort_values("Revenue (latest)", ascending=False)

    st.markdown(
        f'<div style="background:#f0f4ff;border-left:4px solid #4361ee;'
        f'padding:10px 16px;border-radius:4px;margin:8px 0;">'
        f'<b>🔍 {sector_name}</b> — {len(display)} companies'
        f'{"  ·  " + str(filtered["revenue"].notna().sum()) + " with SEC financial data" if filtered["revenue"].notna().any() else ""}'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.dataframe(display, use_container_width=True, hide_index=True, height=320)


# ── Data loader ──────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_FILE)

    companies = pd.read_sql(
        "SELECT cd.id, cd.name, cd.hub, cd.company_size, cd.state, "
        "cc.sector, cc.revenue_model "
        "FROM companies_deduped cd "
        "LEFT JOIN company_classifications cc ON cd.id = cc.company_id "
        "WHERE cd.hub != 'washington-dc'",
        conn,
    )

    sec = pd.read_sql("""
        SELECT sf.company_id, sf.year, sf.revenue, sf.rd_expense, sf.net_income,
               sf.operating_cash_flow, cc.sector
        FROM sec_financials sf
        JOIN company_classifications cc ON sf.company_id = cc.company_id
        WHERE sf.year BETWEEN 2015 AND 2024
    """, conn)

    bls = pd.read_sql("SELECT * FROM bls_employment", conn)

    ai_adoption = pd.read_sql(
        "SELECT * FROM so_ai_adoption ORDER BY year, usage_pct DESC", conn
    )
    salary = pd.read_sql("SELECT * FROM so_salary_trend ORDER BY year", conn)
    devtype = pd.read_sql("SELECT * FROM so_devtype_trend", conn)
    tools = pd.read_sql("SELECT * FROM so_tools_trend", conn)
    desire_gap = pd.read_sql("SELECT * FROM so_desire_gap ORDER BY year, gap DESC", conn)

    # Latest revenue per company (for drill-down table)
    company_revenue = pd.read_sql("""
        SELECT sf.company_id, sf.revenue, sf.rd_expense, sf.year
        FROM sec_financials sf
        INNER JOIN (
            SELECT company_id, MAX(year) AS max_year
            FROM sec_financials WHERE revenue IS NOT NULL
            GROUP BY company_id
        ) latest ON sf.company_id = latest.company_id AND sf.year = latest.max_year
    """, conn)

    # Phase G sector-level Investing metrics (CAGR 40% / SFR 30% / Margin 30%, 2020-2024)
    sector_metrics = pd.read_sql("SELECT * FROM sector_opportunity_metrics", conn)

    conn.close()
    return (companies, sec, bls, ai_adoption, salary, devtype, tools, desire_gap,
            company_revenue, sector_metrics)


(companies, sec, bls, ai_adoption, salary, devtype, tools, desire_gap,
 company_revenue, sector_metrics) = load_data()

# ── Color palette ────────────────────────────────────────────────────────────
SECTOR_COLORS = px.colors.qualitative.Plotly
HUB_LABELS = {
    "seattle": "Seattle", "san-francisco": "San Francisco",
    "new-york-city": "New York", "los-angeles": "Los Angeles",
    "denver": "Denver", "dallas": "Dallas", "chicago": "Chicago",
    "boston": "Boston", "austin": "Austin", "atlanta": "Atlanta",
}
companies["hub_label"] = companies["hub"].map(HUB_LABELS).fillna(companies["hub"])

SIZE_ORDER = ["Startup", "Small", "Mid-size", "Enterprise"]

# ── Session state defaults ────────────────────────────────────────────────────
if "w_inventing" not in st.session_state:
    st.session_state.w_inventing = 40
if "w_investing" not in st.session_state:
    st.session_state.w_investing = 60

# ── Sidebar: Interactive Opportunity Score weights ────────────────────────────
with st.sidebar:
    st.header("⚙️ Opportunity Score Weights")

    # Investor profile buttons MUST come before sliders (Streamlit constraint:
    # session_state keys bound to widgets cannot be modified after widget instantiation)
    st.markdown("**Investor profiles:**")
    pb1, pb2, pb3 = st.columns(3)
    with pb1:
        if st.button("🚀 Growth", use_container_width=True):
            st.session_state.w_inventing = 60
            st.session_state.w_investing = 40
            st.rerun()
    with pb2:
        if st.button("💎 Value", use_container_width=True):
            st.session_state.w_inventing = 20
            st.session_state.w_investing = 80
            st.rerun()
    with pb3:
        if st.button("⚖️ Balanced", use_container_width=True):
            st.session_state.w_inventing = 40
            st.session_state.w_investing = 60
            st.rerun()
    st.caption("Growth: R&D 60 / Cash+Growth 40")
    st.caption("Value: R&D 20 / Cash+Growth 80")
    st.caption("Balanced: R&D 40 / Cash+Growth 60")

    st.divider()
    st.markdown("Adjust to reflect your investment style:")

    w_inventing = st.slider("🔬 Inventing (R&D intensity)", 0, 100, step=5, key="w_inventing")
    w_investing = st.slider("💰 Investing (growth + cash)", 0, 100, step=5, key="w_investing")

    total = w_inventing + w_investing
    if total == 0:
        st.error("Weights must sum to > 0")
        w_inventing, w_investing = 40, 60
        total = 100
    st.caption(f"Total: {total} (auto-normalized to 100%)")

    wi = w_inventing / total
    wv = w_investing / total

st.title("📊 Mapping the U.S. Digital Economy")
st.markdown("**Where is the next investment opportunity in U.S. tech?**")

# ── Dynamic hero metrics ──────────────────────────────────────────────────────
_rv = sec[sec["year"].isin([2019, 2024]) & sec["revenue"].notna()].copy()
_rv_piv = (
    _rv.groupby(["sector", "year"])["revenue"]
    .mean().unstack("year").reset_index()
)
_rv_piv.columns = ["sector", "rev_2019", "rev_2024"]
_rv_piv = _rv_piv.dropna()
_rv_piv["growth_pct"] = (
    (_rv_piv["rev_2024"] - _rv_piv["rev_2019"]) / _rv_piv["rev_2019"] * 100
).round(0)
_top_growth_pct  = int(_rv_piv["growth_pct"].max())
_top_growth_sector = _rv_piv.loc[_rv_piv["growth_pct"].idxmax(), "sector"]

# OCF hero: total operating cash flow across all sampled public tech in latest year
_ocf_latest = sec[sec["operating_cash_flow"].notna()].copy()
if not _ocf_latest.empty:
    _ocf_yr = int(_ocf_latest["year"].max())
    _ocf_total_b = _ocf_latest[_ocf_latest["year"] == _ocf_yr]["operating_cash_flow"].sum() / 1e9
else:
    _ocf_yr, _ocf_total_b = 2024, 0

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"""
    <div class="stat-card">
      <div class="big-stat-label">🏢 Fastest-growing sector</div>
      <div class="big-stat">{_top_growth_pct}%</div>
      <div class="big-stat-label">revenue growth &nbsp;·&nbsp; {_top_growth_sector} &nbsp;·&nbsp; 2019→2024</div>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="stat-card">
      <div class="big-stat-label">💵 Cash generated by sampled public tech</div>
      <div class="big-stat">${_ocf_total_b:,.0f}B</div>
      <div class="big-stat-label">total operating cash flow &nbsp;·&nbsp; 413 firms &nbsp;·&nbsp; {_ocf_yr}</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ── Tabs ─────────────────────────────────────────────────────────────────────
tabs = st.tabs([
    "1️⃣ Market Overview",
    "2️⃣ Technology Trends",
    "3️⃣ Innovation",
    "4️⃣ Capital Flow",
    "5️⃣ Investment Opportunities",
    "6️⃣ Data & Methods",
])


# ════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Market Overview
# ════════════════════════════════════════════════════════════════════════════
with tabs[0]:
    st.header("Market Overview")
    st.markdown("*4,001 U.S. tech companies across 10 major hubs — who are they?*")

    col_a, col_b = st.columns(2)

    # Chart 1: Sector distribution (bar, clickable)
    with col_a:
        st.subheader("Sector Distribution")
        st.caption("💡 Click any bar to see companies in that sector")
        sector_counts = (
            companies.groupby("sector").size().reset_index(name="count")
            .sort_values("count", ascending=True)
        )
        fig1 = px.bar(
            sector_counts, x="count", y="sector",
            orientation="h",
            color="count",
            color_continuous_scale=["#bbdefb", "#1565c0", "#0d47a1"],
            labels={"count": "Number of Companies", "sector": ""},
            height=520,
        )
        fig1.update_layout(coloraxis_showscale=False, margin=dict(l=0))
        sec1_event = st.plotly_chart(
            fig1, use_container_width=True,
            on_select="rerun", selection_mode="points", key="sec1_sector_chart"
        )
        if sec1_event and sec1_event.selection and sec1_event.selection.points:
            clicked = sec1_event.selection.points[0].get("y")
            if clicked:
                show_sector_drilldown(clicked, companies, company_revenue)

        st.markdown("""
        <div class="insight-box">
          <b>What we see:</b> Enterprise / ERP / HRM dominates (902 companies), followed by
          GovTech (516) and Fintech (490). AI-native sectors (AI Assistants, AI Foundation Models)
          are still small in absolute count.<br><br>
          <b>Why it happens:</b> Enterprise software has decades of incumbent players. AI sectors
          are newer and fewer companies have fully committed to AI as a primary product.<br><br>
          <b>What it means:</b> High company count ≠ high growth. The small AI sector count
          signals early-stage market — higher risk, higher upside.
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="takeaway">📌 Enterprise is the largest sector by count, but AI sectors lead in revenue growth.</div>', unsafe_allow_html=True)

    # Chart 2: Hub × Company Size
    with col_b:
        st.subheader("Company Size by Hub")
        size_hub = (
            companies[companies["company_size"].notna()]
            .groupby(["hub_label", "company_size"])
            .size().reset_index(name="count")
        )
        size_hub["company_size"] = pd.Categorical(size_hub["company_size"], categories=SIZE_ORDER, ordered=True)
        size_hub = size_hub.sort_values("company_size")

        fig2 = px.bar(
            size_hub, x="hub_label", y="count", color="company_size",
            barmode="stack",
            category_orders={"company_size": SIZE_ORDER},
            color_discrete_map={
                "Startup": "#90caf9", "Small": "#42a5f5",
                "Mid-size": "#1565c0", "Enterprise": "#0d47a1",
            },
            labels={"hub_label": "", "count": "Companies", "company_size": "Size"},
            height=520,
        )
        fig2.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig2, use_container_width=True)

        st.markdown("""
        <div class="insight-box">
          <b>What we see:</b> All hubs share a similar pattern — Small companies (50–499 employees)
          are the plurality. Boston and San Francisco show notably higher Startup ratios, while
          Dallas and Atlanta lean toward Mid-size and Enterprise.<br><br>
          <b>Why it happens:</b> Coastal innovation hubs (Boston, SF) attract early-stage ventures
          with access to VC capital. Sunbelt cities attract established firms relocating for lower costs.<br><br>
          <b>What it means:</b> Boston and SF offer higher-risk, higher-reward startup exposure.
          Dallas and Atlanta offer more stable, established-company exposure.
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="takeaway">📌 Boston & SF = startup-dense. Dallas & Atlanta = enterprise-dense.</div>', unsafe_allow_html=True)

    # Map: Company density by state
    st.subheader("🗺️ Tech Company Geography — Where Are They?")
    map_col1, map_col2 = st.columns([2, 1])

    with map_col1:
        state_counts = (
            companies[companies["state"].notna()]
            .groupby("state").size().reset_index(name="count")
        )
        state_counts["state_abbrev"] = state_counts["state"].map(STATE_ABBREV)
        state_counts = state_counts.dropna(subset=["state_abbrev"])

        fig_map = px.choropleth(
            state_counts,
            locations="state_abbrev",
            locationmode="USA-states",
            color="count",
            scope="usa",
            color_continuous_scale="Blues",
            labels={"count": "Companies", "state_abbrev": "State"},
            hover_data={"state": True, "count": True, "state_abbrev": False},
            height=420,
        )
        fig_map.update_layout(
            geo=dict(showlakes=True, lakecolor="rgb(255,255,255)"),
            margin=dict(l=0, r=0, t=0, b=0),
            coloraxis_colorbar=dict(title="Companies"),
        )
        st.plotly_chart(fig_map, use_container_width=True)

    with map_col2:
        st.markdown("**Top 10 States**")
        top_states = state_counts.nlargest(10, "count")[["state", "count"]]
        for _, row in top_states.iterrows():
            pct = row["count"] / state_counts["count"].sum() * 100
            st.markdown(f"**{row['state']}** — {int(row['count'])} ({pct:.1f}%)")

        st.markdown("""
        <div class="insight-box" style="margin-top:12px;">
          <b>What we see:</b> California (953) and Texas (638) dominate.
          Washington state punches above its weight due to Seattle's tech scene.<br><br>
          <b>What it means:</b> Coastal concentration remains strong, but Texas
          and Georgia signal Sunbelt emergence as a second-tier tech hub.
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="takeaway">📌 CA + TX + NY + MA = 75% of sampled companies.</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Technology Trends
# ════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.header("Technology Trends")
    st.markdown("*Stack Overflow Developer Survey (2017–2025) · 22,000–50,000 respondents/year*")
    st.info("**Methodology:** SO Survey reflects macro-level technology adoption across developers worldwide. "
            "The Desire Gap below is a leading indicator of where the talent pipeline is forming next.")

    # Desire Gap chart
    st.subheader("📈 Technology Desire Gap — What Developers Want to Learn Next")
    st.markdown(
        "*Professional developers only (MainBranch = 'developer by profession') · "
        "Gap = Want% − Have%. Positive = growing demand ahead.*"
    )
    st.info(
        "**How to read this:** A positive gap means more developers *want* to learn this tool "
        "than currently use it — a leading indicator of future adoption. "
        "A negative gap means the tool is mature/saturated — still widely used, but not exciting new learners."
    )

    gap_year = st.select_slider(
        "Select year:", options=sorted(desire_gap["year"].unique()), value=2024
    )
    gap_plot = desire_gap[desire_gap["year"] == gap_year].copy()
    gap_plot = gap_plot.sort_values("gap", ascending=True)

    colors = ["#e53935" if v < 0 else "#43a047" for v in gap_plot["gap"]]

    fig_gap = go.Figure(go.Bar(
        x=gap_plot["gap"],
        y=gap_plot["tool"],
        orientation="h",
        marker_color=colors,
        text=gap_plot["gap"].apply(lambda x: f"{x:+.1f}%"),
        textposition="outside",
    ))
    fig_gap.add_vline(x=0, line_color="gray", line_width=1)
    fig_gap.update_layout(
        xaxis_title="Desire Gap (Want% − Have%)",
        yaxis_title="",
        height=420,
        margin=dict(l=0, r=60),
        xaxis=dict(zeroline=False),
    )
    st.plotly_chart(fig_gap, use_container_width=True)

    st.markdown("""
    <div class="insight-box">
      <b>What we see:</b> TensorFlow and PyTorch consistently show positive desire gaps —
      more professional developers want to learn them than currently use them.
      React, Angular, and Node.js show negative gaps — mature, widely adopted,
      but not attracting new learners at the same rate.<br><br>
      <b>Why it matters for investors:</b> Positive desire gap = talent pipeline is growing
      for this technology. Sectors relying on PyTorch/TensorFlow (AI, ML infrastructure)
      will continue to attract developer talent, sustaining their competitive advantage.
    </div>
    """, unsafe_allow_html=True)
    st.markdown('<div class="takeaway">📌 PyTorch & TensorFlow: more devs want to learn than currently use — AI/ML talent pipeline keeps growing.</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Innovation (Inventing)
# ════════════════════════════════════════════════════════════════════════════
with tabs[2]:
    st.header("Innovation — Inventing")
    st.markdown("*SEC EDGAR financial data · 2,937 public companies matched out of 4,001*")
    st.warning("⚠️ **Public companies only:** SEC EDGAR covers 2,937 of 4,001 companies (73%). "
               "Private companies — which make up the majority of startups and mid-size firms — "
               "are not captured. R&D intensity figures reflect publicly traded companies only.")
    st.info("**Methodology:** R&D intensity = R&D Expense / Revenue. Higher ratio = more innovation spend "
            "relative to current revenue. This is the 'Inventing' signal.")

    col_a, col_b = st.columns(2)

    # Chart 5: R&D / Revenue ratio by sector
    with col_a:
        st.subheader("R&D Intensity by Sector (2020–2024 avg)")

        rd_ratio = (
            sec[(sec["year"] >= 2020) & sec["revenue"].notna() & sec["rd_expense"].notna()
                & (sec["revenue"] > 0)]
            .copy()
        )
        rd_ratio["rd_intensity"] = rd_ratio["rd_expense"] / rd_ratio["revenue"]
        # Cap at 10x (1000%) to remove outliers from tiny-revenue companies
        rd_ratio = rd_ratio[rd_ratio["rd_intensity"] <= 10]

        rd_by_sector = (
            rd_ratio.groupby("sector")["rd_intensity"]
            .mean().reset_index()
            .sort_values("rd_intensity", ascending=True)
        )
        rd_by_sector["rd_pct"] = (rd_by_sector["rd_intensity"] * 100).round(1)

        fig7 = px.bar(
            rd_by_sector, x="rd_pct", y="sector",
            orientation="h",
            color="rd_pct", color_continuous_scale="Oranges",
            labels={"rd_pct": "R&D / Revenue (%)", "sector": ""},
            height=480,
        )
        top_rd = rd_by_sector.iloc[-1]
        fig7.add_annotation(
            x=top_rd["rd_pct"], y=top_rd["sector"],
            text=f"🔬 {top_rd['rd_pct']:.0f}% of revenue",
            showarrow=True, arrowhead=2, arrowcolor="#e65100",
            font=dict(color="#e65100", size=12, family="Arial Black"),
            xshift=10,
        )
        fig7.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig7, use_container_width=True)

        st.markdown("""
        <div class="insight-box">
          <b>What we see:</b> Search Engines invest the most in R&D relative to revenue (~210%),
          followed by AI Foundation Models. Fintech, E-commerce, and Advertising show the lowest R&D intensity.<br><br>
          <b>Why it happens:</b> Search and AI require massive ongoing R&D to maintain competitive moats.
          Fintech and E-commerce rely more on network effects and scale than continuous R&D.<br><br>
          <b>What it means:</b> High R&D intensity sectors are in technology development mode —
          higher risk but potential for breakthrough competitive advantages.
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="takeaway">📌 AI Foundation Models invest ~200%+ of revenue in R&D — a bet on future dominance, not current profits.</div>', unsafe_allow_html=True)

    # Chart 6: R&D trend over time for top sectors
    with col_b:
        st.subheader("R&D Expense Trend — Top Sectors (2015–2024)")

        top_rd_sectors = rd_by_sector.nlargest(6, "rd_intensity")["sector"].tolist()

        rd_trend = (
            sec[sec["sector"].isin(top_rd_sectors) & sec["rd_expense"].notna()]
            .groupby(["year", "sector"])["rd_expense"].mean()
            .reset_index()
        )
        rd_trend["rd_bn"] = rd_trend["rd_expense"] / 1e9

        fig8 = px.line(
            rd_trend, x="year", y="rd_bn", color="sector",
            markers=True,
            labels={"year": "Year", "rd_bn": "Avg R&D Expense ($B)", "sector": "Sector"},
            height=480,
        )
        st.plotly_chart(fig8, use_container_width=True)

        st.markdown("""
        <div class="insight-box">
          <b>What we see:</b> R&D spending accelerated sharply after 2020 in AI and Advertising sectors.
          The post-2022 surge in AI Foundation Models corresponds directly with the LLM race
          triggered by ChatGPT's launch.<br><br>
          <b>Why it happens:</b> The transformer architecture and scale laws created a winner-take-most
          dynamic. Companies that fall behind in model capability face existential risk.<br><br>
          <b>What it means:</b> R&D spending is not discretionary in AI — it's a survival cost.
          This creates high barriers to entry and long-term moat for current leaders.
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="takeaway">📌 R&D acceleration post-2022 = direct market response to the LLM breakthrough.</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Capital Flow (Investing)
# ════════════════════════════════════════════════════════════════════════════
with tabs[3]:
    st.header("Capital Flow — Investing")
    st.markdown("*Revenue growth (SEC EDGAR) + Labor market signals (BLS)*")
    st.warning("⚠️ **Public companies only:** Revenue figures are from SEC EDGAR (2,937 public companies). "
               "Private company revenue is not included — growth figures represent the publicly traded segment of each sector.")

    # Revenue Growth summary — full chart is in Section 5
    st.info(
        "📈 **Revenue Growth by Sector:** AI Foundation Models +386%, Advertising +374%, "
        "E-learning +311% (2019→2024). All sectors grew. "
        "→ Full breakdown and Opportunity Score analysis in **Section 5: Investment Opportunities**."
    )

    # ── NI vs OCF: why Net Income alone misleads ─────────────────────────────
    st.subheader("Net Income vs Operating Cash Flow — Why NI Alone Misleads")
    st.markdown("*Sector averages, 2020–2024. NI reflects accounting profit; OCF reflects actual cash generated.*")

    # Exclude INSUF sectors (n<5) — their sector means are unstable
    valid_sectors = sector_metrics.loc[
        sector_metrics["insufficient_data"] == 0, "sector"
    ].tolist()

    cashflow_base = sec[
        (sec["year"] >= 2020) & (sec["year"] <= 2024) &
        sec["sector"].isin(valid_sectors)
    ].copy()
    ni_by_sec = cashflow_base.groupby("sector")["net_income"].mean()
    ocf_by_sec = cashflow_base.groupby("sector")["operating_cash_flow"].mean()

    cashflow_df = pd.DataFrame({
        "sector": valid_sectors,
        "ni_avg": [ni_by_sec.get(s, None) for s in valid_sectors],
        "ocf_avg": [ocf_by_sec.get(s, None) for s in valid_sectors],
    }).dropna()
    cashflow_df["ni_b"]  = cashflow_df["ni_avg"]  / 1e9
    cashflow_df["ocf_b"] = cashflow_df["ocf_avg"] / 1e9
    cashflow_df = cashflow_df.sort_values("ocf_b", ascending=False)

    fig_niocf = go.Figure()
    fig_niocf.add_trace(go.Bar(
        x=cashflow_df["sector"], y=cashflow_df["ni_b"],
        name="Net Income", marker_color="#c62828",
        hovertemplate="<b>%{x}</b><br>NI avg: $%{y:.2f}B<extra></extra>",
    ))
    fig_niocf.add_trace(go.Bar(
        x=cashflow_df["sector"], y=cashflow_df["ocf_b"],
        name="Operating Cash Flow", marker_color="#2e7d32",
        hovertemplate="<b>%{x}</b><br>OCF avg: $%{y:.2f}B<extra></extra>",
    ))
    fig_niocf.update_layout(
        barmode="group",
        height=460,
        xaxis_title="",
        yaxis_title="Avg per-company ($B, 2020–2024)",
        xaxis_tickangle=-30,
        legend=dict(orientation="h", y=1.08),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    fig_niocf.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
    st.plotly_chart(fig_niocf, use_container_width=True)

    st.markdown("""
    <div class="insight-box">
      <b>What we see:</b> Several sectors show OCF meaningfully above NI — or OCF positive while NI
      is near zero or negative. That gap = stock-based compensation, depreciation, and working-capital
      effects that depress accounting income but not cash.<br><br>
      <b>Why it matters:</b> NI is manipulable; cash is not. A company "losing money" on the income
      statement can still be generating the cash it needs to fund R&D, pay debt, and return capital.
      Looking at NI alone systematically underweights cash-generative tech.
    </div>
    """, unsafe_allow_html=True)
    st.markdown('<div class="takeaway">📌 NI only = wrong signal. OCF shows the sector is actually self-funding (or not).</div>', unsafe_allow_html=True)

    # ── OCF trend over time ──────────────────────────────────────────────────
    st.subheader("Operating Cash Flow Trend — Top Sectors (2015–2024)")

    ocf_all = sec[sec["operating_cash_flow"].notna() & sec["sector"].isin(valid_sectors)]
    top_ocf_sectors = (
        ocf_all[ocf_all["year"] >= 2020]
        .groupby("sector")["operating_cash_flow"].mean()
        .nlargest(6).index.tolist()
    )
    ocf_trend = (
        ocf_all[ocf_all["sector"].isin(top_ocf_sectors)]
        .groupby(["year", "sector"])["operating_cash_flow"].mean()
        .reset_index()
    )
    ocf_trend["ocf_b"] = ocf_trend["operating_cash_flow"] / 1e9

    fig_ocf = px.line(
        ocf_trend, x="year", y="ocf_b", color="sector",
        markers=True,
        labels={"year": "Year", "ocf_b": "Avg OCF ($B)", "sector": "Sector"},
        height=460,
    )
    fig_ocf.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
    st.plotly_chart(fig_ocf, use_container_width=True)

    st.markdown("""
    <div class="insight-box">
      <b>What we see:</b> OCF trajectories differ from revenue trajectories in several sectors —
      a divergence signals the <em>quality</em> of revenue is changing (margin compression,
      working-capital drag, or improving cash conversion).<br><br>
      <b>What it means:</b> A sector whose OCF is flat or shrinking while revenue climbs is
      a warning sign. A sector whose OCF is compounding faster than revenue is a strong buy signal.
    </div>
    """, unsafe_allow_html=True)

    # BLS Employment growth
    st.subheader("Employment Growth by Tech Sector (BLS, 2015–2024)")
    st.info("**BLS Data Limitation:** BLS uses NAICS industry codes — only clearly aligned sectors are shown. "
            "Emerging cross-cutting sectors (e.g., 'AI assistants') have no direct NAICS equivalent and are excluded "
            "to avoid misrepresentation. This ensures all labor market insights shown are high-confidence mappings.")

    bls_plot = bls[bls["employees"].notna()].copy()
    bls_pivot = bls_plot.groupby(["sector", "year"])["employees"].mean().reset_index()

    # Normalize to 2015 = 100
    base = bls_pivot[bls_pivot["year"] == 2015][["sector", "employees"]].rename(columns={"employees": "base"})
    bls_pivot = bls_pivot.merge(base, on="sector")
    bls_pivot["indexed"] = (bls_pivot["employees"] / bls_pivot["base"] * 100).round(1)

    selected_bls = st.multiselect(
        "Select BLS sectors:",
        options=sorted(bls_pivot["sector"].unique()),
        default=["Data Processing & Hosting (518)", "Computer Systems Design (5415)",
                 "Internet & Info Services (519)", "Telecommunications (517)"],
    )

    if selected_bls:
        bls_filtered = bls_pivot[bls_pivot["sector"].isin(selected_bls)]
        fig_bls = px.line(
            bls_filtered, x="year", y="indexed", color="sector",
            markers=True,
            labels={"year": "Year", "indexed": "Employment Index (2015=100)", "sector": "Sector"},
            height=380,
        )
        fig_bls.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.5)
        st.plotly_chart(fig_bls, use_container_width=True)
        st.markdown('<div class="takeaway">📌 Data Processing & Hosting grew 60%+ since 2015 — the physical infrastructure of the AI economy.</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Investment Opportunities
# ════════════════════════════════════════════════════════════════════════════
with tabs[4]:
    # ── Hero ─────────────────────────────────────────────────────────────────
    st.markdown("""
    <div style="background: linear-gradient(135deg, #1a237e 0%, #283593 60%, #3949ab 100%);
                padding: 36px 40px; border-radius: 12px; margin-bottom: 28px;">
      <h1 style="color:white; margin:0; font-size:2.2em;">🔥 Investment Opportunities</h1>
      <p style="color:#c5cae9; margin:10px 0 0 0; font-size:1.15em;">
        Where should investors look next in the U.S. digital economy?
      </p>
      <p style="color:#9fa8da; margin:6px 0 0 0; font-size:0.95em;">
        Based on technology adoption trends (SO Survey) · R&D intensity (SEC EDGAR) ·
        Revenue growth (SEC EDGAR) · Labor market signals (BLS)
      </p>
    </div>
    """, unsafe_allow_html=True)

    # ── Opportunity Score calculation (2-axis: Inventing + Investing) ────────
    # Inventing score: R&D / Revenue ratio (2020–2024 avg), capped at 10x to match Section 3
    rd_scores = (
        sec[(sec["year"] >= 2020) & sec["revenue"].notna() & sec["rd_expense"].notna()
            & (sec["revenue"] > 0)]
        .copy()
    )
    rd_scores["rd_intensity"] = rd_scores["rd_expense"] / rd_scores["revenue"]
    rd_scores = rd_scores[rd_scores["rd_intensity"] <= 10]
    rd_by_sec = rd_scores.groupby("sector")["rd_intensity"].mean()

    # Investing score: pulled from Phase G sector_opportunity_metrics.
    # Per-company Investing = 0.4*CAGR_n + 0.3*SFR_n + 0.3*Margin_n (2020-2024 window,
    # 5-95th percentile normalized). Sector score = median of per-company scores,
    # already on [0, 1] — no additional min-max normalization applied here.
    sm = sector_metrics.set_index("sector")
    investing_score_map = sm["investing_score"]
    cagr_map = sm["cagr_median"]              # annualized decimal, for scatter/narrative
    sfr_map = sm["sfr_median"]                # OCF / R&D ratio, sector median
    margin_map = sm["margin_median"]          # OCF / Revenue ratio, sector median
    insufficient_map = sm["insufficient_data"]  # 1 if n_scored < 5
    n_scored_map = sm["n_scored"]             # # of companies with full 2020-24 financials

    # Build score dataframe — universe = all sectors in Phase G metrics
    all_sectors = sector_metrics["sector"].tolist()
    score_df = pd.DataFrame({"sector": all_sectors})
    score_df["inventing_raw"] = score_df["sector"].map(rd_by_sec)
    score_df["investing_raw"] = score_df["sector"].map(cagr_map)
    score_df["sfr_raw"] = score_df["sector"].map(sfr_map)
    score_df["margin_raw"] = score_df["sector"].map(margin_map)
    score_df["insufficient_data"] = (
        score_df["sector"].map(insufficient_map).fillna(1).astype(int)
    )
    score_df["n_scored"] = score_df["sector"].map(n_scored_map).fillna(0).astype(int)
    # Phase G sub-scores (each already on [0,1], median of per-company normalized value)
    score_df["cagr_score"]   = score_df["sector"].map(sm["cagr_score"])
    score_df["sfr_score"]    = score_df["sector"].map(sm["sfr_score"])
    score_df["margin_score"] = score_df["sector"].map(sm["margin_score"])

    def minmax(s):
        return (s - s.min()) / (s.max() - s.min())

    score_df["inventing"] = minmax(
        score_df["inventing_raw"].fillna(score_df["inventing_raw"].median())
    )
    score_df["investing"] = score_df["sector"].map(investing_score_map)
    score_df["investing"] = score_df["investing"].fillna(score_df["investing"].median())

    # Final weighted score — 2-axis (Inventing + Investing)
    score_df["opportunity_score"] = (
        wi * score_df["inventing"] +
        wv * score_df["investing"]
    ).round(3)

    score_df = score_df.sort_values("opportunity_score", ascending=False)

    # Sectors with n_scored < 5 (Phase G flag) have unreliable Investing signal
    # and are excluded from ranked lists. They still appear in the full chart,
    # grayed out, so the reader sees the gap rather than silent omission.
    valid_df = score_df[score_df["insufficient_data"] == 0]
    insufficient_df = score_df[score_df["insufficient_data"] == 1]

    # ── Top 3 cards (dynamic — based on actual Opportunity Score) ────────────
    st.markdown("### Top 3 Investment Opportunities")

    if not insufficient_df.empty:
        excluded_names = ", ".join(insufficient_df["sector"].tolist())
        st.caption(
            f"⚠️ Excluded from ranking (n<5 public companies with full financials): "
            f"*{excluded_names}*"
        )

    # Sector context: pre-written insight per sector for any that may appear in top 3.
    # Bullets prefer quantitative signals available in the DB (CAGR, SFR, Cash Margin, R&D intensity).
    SECTOR_CONTEXT = {
        "AI foundation models":         ("High-Growth",      "#2e7d32", "Early-stage infrastructure play",           ["R&D / Revenue ~200% — survival-level reinvestment", "Winner-take-most market structure", "Compute cost is the binding constraint"],     "Companies reaching model-scale economics first will build structural moats.",                  "Regulatory uncertainty · compute cost volatility"),
        "AI assistants & copilots":     ("High-Growth",      "#2e7d32", "Fastest-growing developer tool category",   ["Embedded AI in IDEs, APIs, and workflows", "Multi-model shift — not winner-take-all", "Distribution (not model quality) is now the moat"], "Developer AI tooling is the primary API-call consumer for foundation model APIs.",              "Market consolidation risk · model commoditization"),
        "Developer tooling":            ("Steady Growth",    "#1565c0", "Core infrastructure for every tech sector", ["Strong Self-Funding Ratio — R&D funded by OCF", "Structural demand independent of economic cycle", "Enterprise spend is sticky"],               "Every sector runs on developer tooling; when cycles turn, tooling cuts last.",                  "Cloud cost pressure · open-source substitution"),
        "Cybersecurity & identity":     ("Structural Demand","#e65100", "Non-discretionary spend, regulation-driven",["Cash Margin among highest in sample", "Demand driven by GDPR/CCPA/AI governance", "AI creates new threats AND new tools"],       "Unlike other sectors, demand is structural — companies MUST spend on security.",                "Crowded vendor landscape · consolidation pressure"),
        "E-learning & skill platforms": ("High-Growth",      "#2e7d32", "Post-COVID digital education boom",         ["Subscription model = recurring revenue", "AI upskilling driving net-new demand", "Positive revenue CAGR, 2020–2024"],                    "AI skill gap is creating structural demand for technical education platforms.",                 "Content commoditization · free alternatives"),
        "Advertising & attention":      ("High-Growth",      "#2e7d32", "AI-powered ad targeting surge",             ["Strong Cash Margin — ad revenue converts efficiently", "Programmatic + AI optimization", "Platform consolidation around big players"], "AI dramatically improves ad ROI — driving reinvestment in digital advertising.",              "Privacy regulation · cookie deprecation"),
        "Fintech & payments":           ("Stable Compounder","#1565c0", "Mature infrastructure, reliable cash flows",["Highest SFR — mature cash generation funds all R&D internally", "Low R&D → network effects > innovation", "Predictable transaction-fee revenue"], "Shift from disruption to infrastructure.",                                                       "Regulatory pressure · interest rate sensitivity"),
        "Subscription content":         ("Stable Growth",    "#1565c0", "Recurring revenue, low churn model",        ["Streaming + SaaS hybrid models", "Bundle strategies reducing churn", "OCF trajectory stable through macro cycles"],                       "Subscription model provides revenue predictability — attractive in volatile markets.",         "Content cost inflation · subscriber saturation"),
    }

    top3 = valid_df.head(3)
    medals = ["🥇", "🥈", "🥉"]

    st.markdown(
        '<div style="background:#f5f7ff;padding:24px 20px;border-radius:12px;'
        'border:1px solid #c5cae9;margin-bottom:16px;">',
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns(3)

    for col, (medal, (_, row)) in zip([c1, c2, c3], zip(medals, top3.iterrows())):
        sector = row["sector"]
        score_val = int(row["opportunity_score"] * 100)
        ctx = SECTOR_CONTEXT.get(sector, (
            "Top Sector", "#555", sector,
            [f"Revenue growth: {row['investing_raw']*100:.0f}%" if pd.notna(row['investing_raw']) else "Strong metrics"],
            "High opportunity score across all three dimensions.", "Data coverage limitations",
        ))
        tag, badge_color, headline, bullets, thesis, risk = ctx

        with col:
            st.markdown(f"#### {medal} {sector}")
            st.markdown(
                f'<span style="background:{badge_color};color:white;padding:2px 10px;'
                f'border-radius:12px;font-size:0.8em;">{tag}</span>'
                f'<span style="float:right;font-weight:700;font-size:1.1em;">Score: {score_val}/100</span>',
                unsafe_allow_html=True,
            )
            st.markdown(f"*{headline}*")
            st.progress(score_val / 100)
            for b in bullets:
                st.markdown(f"- {b}")
            st.markdown(f"**Thesis:** {thesis}")
            st.caption(f"⚠️ Risk: {risk}")

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Top 5 Companies in #1 Sector ─────────────────────────────────────────
    top_sector = valid_df.iloc[0]["sector"]
    top_score  = int(valid_df.iloc[0]["opportunity_score"] * 100)

    st.markdown(f"### 🏆 Top Companies in **{top_sector}** (Score: {top_score}/100)")
    st.caption("Ranked by most recent annual revenue (SEC EDGAR). Private companies without SEC filings are excluded.")

    top_co = (
        companies[companies["sector"] == top_sector]
        .merge(company_revenue, left_on="id", right_on="company_id", how="inner")
        .sort_values("revenue", ascending=False)
        .head(5)
    )

    if top_co.empty:
        st.info("No SEC financial data available for this sector.")
    else:
        cards = st.columns(len(top_co))
        rank_colors = ["#f9a825", "#9e9e9e", "#a1660a", "#4361ee", "#4361ee"]
        rank_labels = ["🥇 #1", "🥈 #2", "🥉 #3", "#4", "#5"]

        for col, (rank_lbl, rank_color), (_, row) in zip(
            cards, zip(rank_labels, rank_colors), top_co.iterrows()
        ):
            rev_b  = row["revenue"] / 1e9
            rd_val = row.get("rd_expense")
            rd_str = f"${rd_val/1e9:.1f}B" if pd.notna(rd_val) and rd_val >= 1e9 \
                     else (f"${rd_val/1e6:.0f}M" if pd.notna(rd_val) else "N/A")

            with col:
                st.markdown(
                    f'<div style="background:#fff;border:1px solid #e0e0e0;border-top:4px solid {rank_color};'
                    f'border-radius:8px;padding:16px 14px;text-align:center;">'
                    f'<div style="font-size:0.8em;color:#888;margin-bottom:4px;">{rank_lbl}</div>'
                    f'<div style="font-weight:700;font-size:0.95em;line-height:1.3;margin-bottom:10px;">'
                    f'{row["name"]}</div>'
                    f'<div style="font-size:1.5em;font-weight:800;color:#1a237e;">${rev_b:.1f}B</div>'
                    f'<div style="font-size:0.75em;color:#888;">Revenue ({int(row["year"])})</div>'
                    f'<hr style="margin:10px 0;border-color:#f0f0f0;">'
                    f'<div style="font-size:0.8em;color:#555;">R&D: <b>{rd_str}</b></div>'
                    f'<div style="font-size:0.78em;color:#888;">{row["hub_label"]} · {row["company_size"]}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    # ── Methodology box ──────────────────────────────────────────────────────
    with st.expander("📐 Opportunity Score Methodology"):
        st.markdown(f"""
        **Formula:** `Opportunity Score = {wi:.0%} × Inventing + {wv:.0%} × Investing`
        *(Adjust weights in the sidebar ←)*

        | Component | Source | Metric | Weight |
        |-----------|--------|--------|--------|
        | **Inventing** | SEC EDGAR 2020–2024 | Avg R&D Expense / Revenue ratio | {wi:.0%} |
        | **Investing** | SEC EDGAR 2020–2024 | Composite: 40% CAGR + 30% SFR + 30% Cash Margin | {wv:.0%} |

        **Investing composite (per company, then sector median):**

        | Sub-metric | Weight | Definition |
        |------------|-------:|------------|
        | Revenue CAGR | 40% | Annualized revenue growth, 2020→2024 |
        | Self-Funding Ratio (SFR) | 30% | Operating Cash Flow ÷ R&D Expense (can this sector fund its own R&D?) |
        | Cash Margin | 30% | Operating Cash Flow ÷ Revenue (how efficiently does revenue convert to cash?) |

        Each sub-metric is 5th–95th percentile winsorized and min-max normalized across companies,
        then combined. Sector Investing score = **median** of per-company composite scores (robust to outliers).

        Inventing is min-max normalized across sectors. Investing arrives from the pipeline
        already on [0, 1] — no additional normalization applied.

        **Insufficient data flag:** sectors with fewer than 5 public companies carrying full 2020–2024
        financials are flagged as insufficient (gray in the ranking chart, excluded from Top 3 / Top 5).
        Currently 4 sectors: *AI foundation models* (n=3), *AI assistants & copilots* (n=4),
        *Creative & design tools* (n=1), *Search engines* (n=1). These still have meaningful qualitative
        signals but lack statistical power for the quantitative layers.

        **Learning layer (planned):** A quantitative Learning score derived from Stack Overflow Survey
        data is planned for the next development phase. Currently the score uses Inventing and
        Investing only.

        **Data note:** Sectors without SEC data use median imputation for Inventing.
        BLS labor market data is shown separately (Section 4) for sectors with clear NAICS alignment.
        """)

    # ── Chart 9: Opportunity Score ranking ───────────────────────────────────
    st.markdown("### 📊 The Evidence — What the Data Says")
    st.markdown("*Each chart below supports the top 3 conclusions above.*")
    col_a, col_b = st.columns([3, 2])

    with col_a:
        st.subheader("Opportunity Score Ranking")
        st.caption("💡 Click any bar to drill down into companies in that sector")

        # Ordering: INSUF sectors pinned to BOTTOM regardless of score (their score
        # is statistically unreliable — putting them at top reads as "best" even when
        # grayed). Valid sectors sit above, ascending by score so the highest valid
        # sector lands at the visual top of the chart.
        valid_sorted = (score_df[score_df["insufficient_data"] == 0]
                        .sort_values("opportunity_score", ascending=True))
        insuf_sorted = (score_df[score_df["insufficient_data"] == 1]
                        .sort_values("opportunity_score", ascending=True))
        bar_df = pd.concat([insuf_sorted, valid_sorted], ignore_index=True)
        bar_df["y_label"] = bar_df.apply(
            lambda r: f"{r['sector']} (n={r['n_scored']}, insufficient)"
            if r["insufficient_data"] == 1 else r["sector"],
            axis=1,
        )

        valid_scores = bar_df.loc[bar_df["insufficient_data"] == 0, "opportunity_score"]
        if len(valid_scores) > 0:
            s_min, s_max = valid_scores.min(), valid_scores.max()
            span = (s_max - s_min) if s_max > s_min else 1.0
        else:
            s_min, span = 0.0, 1.0

        bar_colors = []
        for _, r in bar_df.iterrows():
            if r["insufficient_data"] == 1:
                bar_colors.append("#cccccc")
            else:
                t = (r["opportunity_score"] - s_min) / span
                bar_colors.append(
                    px.colors.sample_colorscale("RdYlGn", [float(np.clip(t, 0, 1))])[0]
                )

        fig11 = go.Figure(
            go.Bar(
                x=bar_df["opportunity_score"],
                y=bar_df["y_label"],
                orientation="h",
                marker_color=bar_colors,
                customdata=bar_df["sector"],
                hovertemplate="<b>%{customdata}</b><br>Score: %{x:.3f}<extra></extra>",
            )
        )
        fig11.update_layout(
            height=520,
            xaxis_title="Opportunity Score (0–1)",
            yaxis_title="",
            yaxis=dict(categoryorder="array", categoryarray=bar_df["y_label"].tolist()),
            margin=dict(l=10, r=10, t=10, b=10),
        )
        sec5_event = st.plotly_chart(
            fig11, use_container_width=True,
            on_select="rerun", selection_mode="points", key="sec5_score_chart"
        )
        if sec5_event and sec5_event.selection and sec5_event.selection.points:
            pt = sec5_event.selection.points[0]
            clicked = pt.get("customdata") or pt.get("y")
            if clicked:
                show_sector_drilldown(clicked, companies, company_revenue)

    with col_b:
        st.subheader("Top 5 Sectors")
        for _, row in valid_df.head(5).iterrows():
            score_pct = int(row["opportunity_score"] * 100)
            bar = "█" * (score_pct // 5) + "░" * (20 - score_pct // 5)
            st.markdown(f"**{row['sector']}**  \n`{bar}` {score_pct}/100")
            st.caption(
                f"Inventing: {row['inventing']:.2f} | "
                f"Investing: {row['investing']:.2f}"
            )

        # Dynamic insight text — reflects current valid_df ranking, not stale hardcode.
        if len(valid_df) >= 3:
            t1, t2, t3 = valid_df.iloc[0], valid_df.iloc[1], valid_df.iloc[2]
            component_labels = {
                "inventing": "R&D intensity (Inventing)",
                "investing": "growth & cash generation (Investing)",
            }
            t1_strength = max(component_labels, key=lambda k: t1[k])
            st.markdown(f"""
            <div class="insight-box">
              <b>What we see:</b> <b>{t1['sector']}</b> leads the ranking
              (score {int(t1['opportunity_score']*100)}/100),
              followed by <b>{t2['sector']}</b> ({int(t2['opportunity_score']*100)}/100)
              and <b>{t3['sector']}</b> ({int(t3['opportunity_score']*100)}/100).
              {t1['sector']}'s edge comes primarily from <b>{component_labels[t1_strength]}</b>.
              <br><br>
              <b>What it means:</b> Sectors scoring high on both layers —
              R&D investment and sustainable cash generation —
              are positioned for durable growth, not just momentum.
              <br><br>
              <b>Note:</b> {len(insufficient_df)} sector{"s" if len(insufficient_df) != 1 else ""}
              with &lt;5 public companies are shown in gray and excluded from the top ranking.
            </div>
            """, unsafe_allow_html=True)

    # ── Chart 10: Growth vs Stability scatter ────────────────────────────────
    st.subheader("Growth vs. Stability Matrix")
    st.markdown("*X-axis: Revenue CAGR 2020–2024 (Investing) | Y-axis: R&D Intensity (Inventing) | Size: Company count*")

    scatter_df = score_df.merge(
        companies.groupby("sector").size().reset_index(name="company_count"),
        on="sector", how="left"
    ).dropna(subset=["investing_raw", "inventing_raw"])

    scatter_df["rev_growth_pct"] = scatter_df["investing_raw"] * 100

    fig12 = px.scatter(
        scatter_df,
        x="rev_growth_pct", y="inventing_raw",
        size="company_count", color="opportunity_score",
        hover_name="sector",
        text="sector",
        color_continuous_scale="RdYlGn",
        labels={
            "rev_growth_pct": "Revenue CAGR 2020–2024 (%, annualized)",
            "inventing_raw": "R&D / Revenue Ratio",
            "opportunity_score": "Opp. Score",
        },
        height=520,
        size_max=50,
    )
    fig12.update_traces(textposition="top center", textfont=dict(size=10))
    # Quadrant lines
    med_x = scatter_df["rev_growth_pct"].median()
    med_y = scatter_df["inventing_raw"].median()
    fig12.add_vline(x=med_x, line_dash="dash", line_color="gray", opacity=0.4)
    fig12.add_hline(y=med_y, line_dash="dash", line_color="gray", opacity=0.4)
    fig12.add_annotation(x=scatter_df["rev_growth_pct"].max() * 0.9, y=scatter_df["inventing_raw"].max() * 0.9,
                         text="🔥 High Growth<br>High Innovation", showarrow=False,
                         font=dict(color="#2e7d32", size=11))
    fig12.add_annotation(x=scatter_df["rev_growth_pct"].max() * 0.85, y=med_y * 0.2,
                         text="💰 High Growth<br>Low R&D (Mature)", showarrow=False,
                         font=dict(color="#1565c0", size=11))
    st.plotly_chart(fig12, use_container_width=True)

    # ── Chart 11: Investing Layer Decomposition ──────────────────────────────
    st.subheader("Investing Layer — What's Driving Each Sector?")
    st.markdown(
        "*Investing score = 40% Revenue CAGR + 30% Self-Funding Ratio + 30% Cash Margin. "
        "The mix tells you **why** a sector ranks high — growth-driven vs. cash-generation-driven.*"
    )

    inv_df = (
        valid_df.dropna(subset=["cagr_score", "sfr_score", "margin_score"])
                .sort_values("investing", ascending=True)
                .copy()
    )
    inv_df["cagr_contrib"]   = inv_df["cagr_score"]   * 0.4
    inv_df["sfr_contrib"]    = inv_df["sfr_score"]    * 0.3
    inv_df["margin_contrib"] = inv_df["margin_score"] * 0.3

    fig13 = go.Figure()
    fig13.add_trace(go.Bar(
        y=inv_df["sector"], x=inv_df["cagr_contrib"], name="Revenue CAGR (40%)",
        orientation="h", marker_color="#2e7d32",
        hovertemplate="<b>%{y}</b><br>CAGR contribution: %{x:.3f}<extra></extra>",
    ))
    fig13.add_trace(go.Bar(
        y=inv_df["sector"], x=inv_df["sfr_contrib"], name="Self-Funding Ratio (30%)",
        orientation="h", marker_color="#1565c0",
        hovertemplate="<b>%{y}</b><br>SFR contribution: %{x:.3f}<extra></extra>",
    ))
    fig13.add_trace(go.Bar(
        y=inv_df["sector"], x=inv_df["margin_contrib"], name="Cash Margin (30%)",
        orientation="h", marker_color="#f57c00",
        hovertemplate="<b>%{y}</b><br>Margin contribution: %{x:.3f}<extra></extra>",
    ))
    fig13.update_layout(
        barmode="stack",
        height=460,
        xaxis_title="Weighted contribution to Investing score",
        yaxis_title="",
        legend=dict(orientation="h", y=1.08),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig13, use_container_width=True)

    st.markdown("""
    <div class="insight-box">
      <b>How to read this:</b> Each sector's bar length equals its Investing score.
      A sector dominated by green (CAGR) is a growth story; dominated by blue (SFR) means
      the sector generates enough cash to fund its own R&D; dominated by orange (Cash Margin)
      means revenue converts efficiently to cash. Balanced bars = healthiest fundamentals.
    </div>
    """, unsafe_allow_html=True)

    # ── Chart 12: Overall Opportunity Score Decomposition ────────────────────
    st.subheader("Opportunity Score — Full Decomposition")
    st.markdown(
        f"*Weighted contributions across both layers "
        f"(Inventing {wi:.0%} + Investing {wv:.0%}).*"
    )

    dec_df = valid_df.sort_values("opportunity_score", ascending=True).copy()
    dec_df["I_contrib"] = dec_df["inventing"] * wi
    dec_df["V_contrib"] = dec_df["investing"] * wv

    fig14 = go.Figure()
    fig14.add_trace(go.Bar(
        y=dec_df["sector"], x=dec_df["I_contrib"], name=f"Inventing ({wi:.0%})",
        orientation="h", marker_color="#00838f",
        hovertemplate="<b>%{y}</b><br>Inventing contribution: %{x:.3f}<extra></extra>",
    ))
    fig14.add_trace(go.Bar(
        y=dec_df["sector"], x=dec_df["V_contrib"], name=f"Investing ({wv:.0%})",
        orientation="h", marker_color="#c62828",
        hovertemplate="<b>%{y}</b><br>Investing contribution: %{x:.3f}<extra></extra>",
    ))
    fig14.update_layout(
        barmode="stack",
        height=460,
        xaxis_title="Weighted contribution to Opportunity Score",
        yaxis_title="",
        legend=dict(orientation="h", y=1.08),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig14, use_container_width=True)

    # ── Data Limitations ──────────────────────────────────────────────────────
    st.divider()
    st.subheader("⚠️ Data Limitations")
    col_l1, col_l2, col_l3 = st.columns(3)
    with col_l1:
        st.markdown("""
        <div class="limit-box">
          <b>SEC EDGAR (73.4% coverage)</b><br>
          2,937 of 4,001 companies matched.
          Only publicly traded companies file with SEC.
          Private startups may be excluded.
          Financial data reflects public companies — likely biases toward larger, more established firms.
        </div>
        """, unsafe_allow_html=True)
    with col_l2:
        st.markdown("""
        <div class="limit-box">
          <b>Sampling Bias (Equal Hub Sampling)</b><br>
          Each hub contributed ~400 companies regardless of actual market size.
          San Francisco has far more tech companies than Denver in reality.
          Hub comparisons show <em>composition</em>, not absolute market size.
        </div>
        """, unsafe_allow_html=True)
    with col_l3:
        st.markdown("""
        <div class="limit-box">
          <b>SO Survey (Developer self-selection)</b><br>
          Respondents are English-speaking, tech-interested developers.
          Overrepresents web/software developers vs embedded/enterprise.
          Survey methodology changed across years — some metrics not directly comparable.
          BLS data only covers clearly NAICS-aligned sectors.
        </div>
        """, unsafe_allow_html=True)

    st.markdown("""
    <div style="text-align:center; padding:20px; color:#666; font-size:0.85em;">
      Built by Sunghun Kim · Calvin University · Senior Project 2026<br>
      Data: builtin.com (scraped) · SEC EDGAR · BLS · Stack Overflow Developer Survey<br>
      4,001 companies · 10 U.S. tech hubs · 3-axis analysis framework
    </div>
    """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 6 — Data & Methods
# ════════════════════════════════════════════════════════════════════════════
with tabs[5]:
    st.header("Data & Methods")
    st.markdown("*How this dashboard was built — data sources, pipeline, and schema.*")

    # ── Data source cards ────────────────────────────────────────────────────
    st.subheader("📦 Data Sources")
    ds1, ds2, ds3, ds4 = st.columns(4)

    with ds1:
        st.markdown("""
        <div style="background:#e8f4fd;border-radius:10px;padding:18px;height:260px;">
          <h4 style="margin:0 0 8px 0;">🏢 builtin.com</h4>
          <b>Type:</b> Web scraping (Playwright)<br><br>
          <b>Coverage:</b> 10 U.S. tech hubs<br>
          <b>Raw rows:</b> 7,601<br>
          <b>After dedup:</b> 4,001 companies<br><br>
          <b>Fields:</b> name, location, employees, description, hub, URL<br><br>
          <b>License:</b> Public company directory — aggregated only
        </div>
        """, unsafe_allow_html=True)

    with ds2:
        st.markdown("""
        <div style="background:#e8f5e9;border-radius:10px;padding:18px;height:260px;">
          <h4 style="margin:0 0 8px 0;">📈 SEC EDGAR</h4>
          <b>Type:</b> REST API (free, no key)<br><br>
          <b>Coverage:</b> Public U.S. companies<br>
          <b>Matched:</b> 481 / 4,001 (12.0%)<br>
          <b>Years:</b> 2015 – 2024<br><br>
          <b>Fields:</b> Revenue, R&D Expense, Net Income, Operating Cash Flow (annual 10-K)<br><br>
          <b>API:</b> data.sec.gov/api/xbrl/companyfacts
        </div>
        """, unsafe_allow_html=True)

    with ds3:
        st.markdown("""
        <div style="background:#fff8e1;border-radius:10px;padding:18px;height:260px;">
          <h4 style="margin:0 0 8px 0;">👷 BLS</h4>
          <b>Type:</b> REST API (free, no key)<br><br>
          <b>Coverage:</b> 11 NAICS sectors<br>
          <b>Rows:</b> 110<br>
          <b>Years:</b> 2015 – 2025<br><br>
          <b>Fields:</b> Employment (thousands), Avg Hourly Wage<br><br>
          <b>Note:</b> NAICS-only — emerging sectors (AI, etc.) have no direct equivalent
        </div>
        """, unsafe_allow_html=True)

    with ds4:
        st.markdown("""
        <div style="background:#fce4ec;border-radius:10px;padding:18px;height:260px;">
          <h4 style="margin:0 0 8px 0;">💻 SO Survey</h4>
          <b>Type:</b> CSV download (public)<br><br>
          <b>Coverage:</b> 2017 – 2025<br>
          <b>Respondents:</b> 22k – 50k / year<br>
          <b>Rows analyzed:</b> ~290k total<br><br>
          <b>Fields:</b> Tools used, AI tools, salary, developer type<br><br>
          <b>Source:</b> insights.stackoverflow.com/survey
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ── Pipeline ─────────────────────────────────────────────────────────────
    st.subheader("⚙️ Data Pipeline")
    st.markdown("""
    <div style="background:#f5f5f5;border-radius:10px;padding:20px;font-family:monospace;font-size:0.92em;line-height:1.9;">
      <b>Step 1 — Scrape</b><br>
      &nbsp;&nbsp;Playwright → builtin.com/companies/location/{hub}<br>
      &nbsp;&nbsp;19 hubs × ~400 companies = 7,601 raw rows → companies_raw<br><br>

      <b>Step 2 — Clean</b> &nbsp;(pipeline/clean_sqlite.py)<br>
      &nbsp;&nbsp;Dedup by builtin_url → 4,001 unique → companies_deduped<br>
      &nbsp;&nbsp;Normalize employees → company_size (Startup / Small / Mid-size / Enterprise)<br>
      &nbsp;&nbsp;Extract state from location string (NULL = 0)<br><br>

      <b>Step 3 — Classify</b> &nbsp;(pipeline/classify_companies.py)<br>
      &nbsp;&nbsp;Claude Haiku API — batch classify each company description<br>
      &nbsp;&nbsp;→ sector (16 categories) + revenue_model (5 categories)<br>
      &nbsp;&nbsp;Cost: ~$4 for 4,001 companies<br><br>

      <b>Step 4 — Enrich: SEC EDGAR</b> &nbsp;(pipeline/enrich_sec.py + phase_a–d_*.py)<br>
      &nbsp;&nbsp;Search CIK by company name → fetch XBRL companyfacts JSON<br>
      &nbsp;&nbsp;Extract Revenue / R&D / Net Income (annual 10-K, FY)<br>
      &nbsp;&nbsp;CIK matching refined via 4-phase cleanup: token overlap, strict rematch via<br>
      &nbsp;&nbsp;company_tickers.json, SIC/revenue sanity check.<br>
      &nbsp;&nbsp;Original 2,937 matches reduced to 481 validated.<br>
      &nbsp;&nbsp;→ sec_financials (4,161 rows) + sec_cik_map (481 rows)<br><br>

      <b>Step 5 — Enrich: BLS</b> &nbsp;(pipeline/enrich_bls.py)<br>
      &nbsp;&nbsp;BLS CES series API → 11 NAICS sectors, 2015–2025<br>
      &nbsp;&nbsp;Average monthly → annual → bls_employment (110 rows)<br><br>

      <b>Step 6 — Survey Analysis</b> &nbsp;(pipeline/analyze_so_survey.py)<br>
      &nbsp;&nbsp;Parse SO Survey CSVs (2017–2025) → 4 tables:<br>
      &nbsp;&nbsp;so_tools_trend · so_ai_adoption · so_salary_trend · so_devtype_trend<br><br>

      <b>Step 6.5 — OCF Enrichment</b> &nbsp;(pipeline/phase_f_ocf.py)<br>
      &nbsp;&nbsp;XBRL: NetCashProvidedByUsedInOperatingActivities + 2 fallbacks<br>
      &nbsp;&nbsp;413 / 481 companies enriched → operating_cash_flow column<br><br>

      <b>Step 6.6 — Opportunity Metrics</b> &nbsp;(pipeline/phase_g_opportunity.py)<br>
      &nbsp;&nbsp;Per-company composite (CAGR 40% + SFR 30% + Cash Margin 30%, 2020–2024)<br>
      &nbsp;&nbsp;→ sector_opportunity_metrics (17 rows, sector-level median + INSUF flag)<br><br>

      <b>Step 7 — Dashboard</b> &nbsp;(dashboard.py)<br>
      &nbsp;&nbsp;Streamlit + Plotly — reads from companies.db via SQLite
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # ── DB Schema ─────────────────────────────────────────────────────────────
    st.subheader("🗄️ Database Schema")
    st.markdown("*All data stored in a single SQLite file: `data/companies.db`*")

    schema_data = {
        "Table": [
            "companies_raw", "companies_deduped", "company_classifications",
            "sec_financials", "sec_cik_map", "sector_opportunity_metrics",
            "bls_employment",
            "so_tools_trend", "so_ai_adoption", "so_salary_trend", "so_devtype_trend",
        ],
        "Rows": ["7,601", "4,001", "4,001", "4,161", "481", "17", "110", "57", "63", "9", "234"],
        "Key Columns": [
            "name, location, employees, description, hub, builtin_url",
            "id, name, hub, state, company_size, employees_count",
            "company_id → sector (16), revenue_model (5)",
            "company_id, year, revenue, rd_expense, net_income, operating_cash_flow",
            "company_id, cik, matched_name",
            "sector, n_scored, cagr/sfr/margin medians & scores, investing_score, insufficient_data",
            "sector (NAICS), year, employees (k), avg_hourly_wage",
            "year, tool, usage_pct",
            "year, ai_tool, usage_pct",
            "year, median_salary, p25_salary, p75_salary, respondents",
            "year, dev_type, count, pct",
        ],
        "Layer": [
            "Company", "Company", "Company",
            "Financial", "Financial", "Financial",
            "Labor Market",
            "Developer Survey", "Developer Survey", "Developer Survey", "Developer Survey",
        ],
    }
    schema_df = pd.DataFrame(schema_data)
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

    st.divider()

    # ── Sample data ───────────────────────────────────────────────────────────
    st.subheader("🔍 Sample Data")

    tab_s1, tab_s2, tab_s3 = st.tabs(["Companies", "SEC Financials", "SO Survey (Dev Types)"])

    with tab_s1:
        sample_companies = companies[
            ["name", "hub_label", "state", "company_size", "sector", "revenue_model"]
        ].dropna(subset=["sector"]).head(10)
        st.dataframe(sample_companies, use_container_width=True, hide_index=True)

    with tab_s2:
        conn_s = sqlite3.connect(DB_FILE)
        sample_sec = pd.read_sql("""
            SELECT cd.name, sf.year, sf.revenue, sf.rd_expense, sf.net_income, cc.sector
            FROM sec_financials sf
            JOIN companies_deduped cd ON sf.company_id = cd.id
            JOIN company_classifications cc ON sf.company_id = cc.company_id
            WHERE sf.revenue IS NOT NULL
            ORDER BY sf.revenue DESC
            LIMIT 10
        """, conn_s)
        conn_s.close()
        sample_sec["revenue"] = (sample_sec["revenue"] / 1e9).round(2).astype(str) + "B"
        sample_sec["rd_expense"] = (sample_sec["rd_expense"] / 1e9).round(2).astype(str) + "B"
        sample_sec["net_income"] = (sample_sec["net_income"] / 1e9).round(2).astype(str) + "B"
        st.dataframe(sample_sec, use_container_width=True, hide_index=True)

    with tab_s3:
        st.markdown("**Developer type distribution by year (top roles)**")
        devtype_sample = (
            devtype[devtype["year"].isin([2019, 2021, 2023, 2025])]
            .sort_values(["year", "pct"], ascending=[True, False])
            .groupby("year").head(5)
            [["year", "dev_type", "count", "pct"]]
            .rename(columns={"year": "Year", "dev_type": "Role", "count": "Count", "pct": "% of Respondents"})
        )
        st.dataframe(devtype_sample, use_container_width=True, hide_index=True)
