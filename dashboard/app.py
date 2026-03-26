"""
Mapping Michigan's Digital Economy — Interactive Dashboard
"""

import os
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Michigan's Digital Economy",
    page_icon="🗺️",
    layout="wide",
)

# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "michigan_tech_map"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    )

@st.cache_data(ttl=60)
def load_companies():
    conn = get_conn()
    return pd.read_sql("SELECT * FROM companies ORDER BY name", conn)

@st.cache_data(ttl=60)
def load_github():
    conn = get_conn()
    return pd.read_sql("SELECT * FROM github_stats ORDER BY total_stars DESC", conn)

@st.cache_data(ttl=60)
def load_tech_stack():
    conn = get_conn()
    return pd.read_sql("SELECT * FROM tech_stack", conn)

companies  = load_companies()
github     = load_github()
tech_stack = load_tech_stack()

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("🗺️ Michigan's Digital Economy")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Map", "Learning", "Inventing", "Investing"],
)

# City → (lat, lon)
CITY_COORDS = {
    "Detroit":          (42.3314, -83.0458),
    "Ann Arbor":        (42.2808, -83.7430),
    "Grand Rapids":     (42.9634, -85.6681),
    "Kalamazoo":        (42.2917, -85.5872),
    "Troy":             (42.6064, -83.1498),
    "Okemos":           (42.7245, -84.4275),
    "Northville":       (42.4314, -83.4835),
    "Lansing":          (42.7325, -84.5555),
    "Flint":            (43.0125, -83.6875),
}

# ── OVERVIEW ─────────────────────────────────────────────────────────────────
if page == "Overview":
    st.title("Mapping Michigan's Digital Economy")
    st.caption("An interactive map of Michigan's tech industry landscape.")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Companies", len(companies))
    col2.metric("GitHub Orgs Found", int((github["found"] == True).sum()))
    col3.metric("Total GitHub Stars", f"{github['total_stars'].sum():,}")
    col4.metric("Sectors Covered", companies["sectors"].str.split("•").explode().str.strip().nunique())

    st.divider()

    # Sector distribution
    sector_series = companies["sectors"].dropna().str.split("•").explode().str.strip()
    sector_counts = sector_series.value_counts().head(15).reset_index()
    sector_counts.columns = ["sector", "count"]

    st.subheader("Sector Distribution")
    fig = px.bar(
        sector_counts, x="count", y="sector", orientation="h",
        color="count", color_continuous_scale="Blues",
        labels={"count": "Companies", "sector": ""},
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False, height=450)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Company Table")
    st.dataframe(
        companies[["name", "city", "employees", "sectors", "invent_category"]],
        use_container_width=True,
        hide_index=True,
    )

# ── MAP ──────────────────────────────────────────────────────────────────────
elif page == "Map":
    st.title("Company Locations")
    st.caption("Geographic distribution of 15 Michigan-connected tech companies.")

    map_df = companies.copy()
    map_df["lat"] = map_df["city"].map(lambda c: CITY_COORDS.get(c, (None, None))[0])
    map_df["lon"] = map_df["city"].map(lambda c: CITY_COORDS.get(c, (None, None))[1])
    map_df = map_df.dropna(subset=["lat", "lon"])

    fig = px.scatter_mapbox(
        map_df,
        lat="lat", lon="lon",
        hover_name="name",
        hover_data={"city": True, "sectors": True, "employees": True, "lat": False, "lon": False},
        color="invent_category",
        size_max=15,
        zoom=6,
        center={"lat": 42.7, "lon": -84.5},
        height=600,
        mapbox_style="carto-positron",
        title="Michigan Tech Company Map",
    )
    fig.update_traces(marker=dict(size=14))
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        map_df[["name", "city", "invent_category", "employees"]],
        use_container_width=True,
        hide_index=True,
    )

# ── LEARNING ─────────────────────────────────────────────────────────────────
elif page == "Learning":
    st.title("Learning — Technology Adoption")
    st.caption("What technologies are Michigan tech companies using?")

    col1, col2 = st.columns(2)
    col1.metric("Companies", len(companies))
    col2.metric("Unique Technologies", companies["tech_stack"].dropna().str.split(",").explode().str.strip().nunique())

    st.divider()

    # Tech stack bar chart
    fw_series = companies["tech_stack"].dropna()
    fw_series = fw_series[fw_series != ""]
    fw_counts = fw_series.str.split(",").explode().str.strip().value_counts().reset_index()
    fw_counts.columns = ["technology", "count"]

    st.subheader("Technologies Adopted")
    fig = px.bar(
        fw_counts, x="technology", y="count",
        color="count", color_continuous_scale="Teal",
        labels={"count": "Companies using", "technology": ""},
    )
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Company Tech Stack Detail")
    st.dataframe(
        companies[["name", "tech_stack"]].rename(columns={"name": "company", "tech_stack": "technologies"}),
        use_container_width=True,
        hide_index=True,
    )

# ── INVENTING ─────────────────────────────────────────────────────────────────
elif page == "Inventing":
    st.title("Inventing — What Are They Building?")
    st.caption("Technology domains Michigan companies are innovating in.")

    cat_series = companies["invent_category"].dropna().str.split("|").explode().str.strip()
    cat_counts = cat_series.value_counts().reset_index()
    cat_counts.columns = ["category", "count"]

    col1, col2 = st.columns([1, 1])

    with col1:
        fig = px.pie(
            cat_counts, names="category", values="count",
            title="Inventing Category Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.bar(
            cat_counts.sort_values("count", ascending=True),
            x="count", y="category", orientation="h",
            color="count", color_continuous_scale="Greens",
            title="Category Breakdown",
            labels={"count": "Companies", "category": ""},
        )
        fig2.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.subheader("Companies by Category")
    selected_cat = st.selectbox("Filter by category", ["All"] + sorted(cat_counts["category"].tolist()))
    filtered = companies.copy()
    if selected_cat != "All":
        filtered = filtered[filtered["invent_category"].str.contains(selected_cat, na=False)]

    st.dataframe(
        filtered[["name", "city", "invent_category", "description"]],
        use_container_width=True,
        hide_index=True,
    )

# ── INVESTING ─────────────────────────────────────────────────────────────────
elif page == "Investing":
    st.title("Investing — Capital Flow")
    st.caption("Funding rounds and investment landscape.")

    funded = companies[companies["total_funding"].notna() & (companies["total_funding"] != "")].copy()

    # Round type distribution
    round_counts = funded["latest_round_type"].value_counts().reset_index()
    round_counts.columns = ["round_type", "count"]

    col1, col2 = st.columns([1, 1])

    with col1:
        fig = px.pie(
            round_counts, names="round_type", values="count",
            title="Funding Stage Distribution",
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.bar(
            round_counts, x="round_type", y="count",
            color="count", color_continuous_scale="Oranges",
            title="Funding Rounds Count",
            labels={"count": "Companies", "round_type": ""},
        )
        fig2.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.subheader("Company Funding Details")
    st.dataframe(
        funded[["name", "total_funding", "latest_round_type", "latest_round_date", "key_investors"]],
        use_container_width=True,
        hide_index=True,
    )
