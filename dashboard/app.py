"""
TagMarshal Data Quality Dashboard

A Streamlit dashboard to demonstrate data quality and usability
for the pilot course data migration project.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# Set Plotly to use light theme
pio.templates.default = "plotly_white"

# =============================================================================
# Configuration
# =============================================================================

st.set_page_config(
    page_title="TagMarshal Data Quality",
    page_icon="⛳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for light theme styling
st.markdown(
    """
<style>
    /* Light theme overrides */
    .stApp {
        background-color: #ffffff;
    }
    
    /* Main container */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Metric cards - light theme */
    [data-testid="stMetric"] {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    /* Code blocks in expanders - light theme */
    .stExpander pre {
        background-color: #f6f8fa;
        border: 1px solid #e1e4e8;
        border-radius: 6px;
    }
    
    /* Table styling */
    .dataframe {
        font-size: 14px;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #1a1a2e;
    }
</style>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# Database Connection
# =============================================================================


def get_trino_connection():
    """Create a connection to Trino."""
    return connect(
        host="localhost",
        port=8080,
        user="trino",
        catalog="iceberg",
        schema="silver",
    )


def check_trino_connection() -> bool:
    """Check if Trino is available."""
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
        return True
    except Exception:
        return False


def run_query(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame."""
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        # Don't show error for each query - we'll show one central message
        return pd.DataFrame()
        return pd.DataFrame()


def show_sql(sql: str, title: str = "SQL Query"):
    """Display SQL in a collapsible container."""
    with st.expander(f"📋 {title}", expanded=False):
        st.code(sql, language="sql")


# =============================================================================
# SQL Queries
# =============================================================================

SQL_EXECUTIVE_SUMMARY = """
SELECT 
    COUNT(DISTINCT course_id) as total_courses,
    COUNT(DISTINCT round_id) as total_rounds,
    COUNT(*) as total_events
FROM iceberg.silver.fact_telemetry_event
"""

SQL_DATA_QUALITY_OVERVIEW = """
SELECT 
    course_id,
    total_events,
    total_rounds,
    ROUND(data_quality_score, 1) as data_quality_score,
    ROUND(pct_missing_pace, 1) as pct_missing_pace,
    ROUND(pct_missing_hole, 1) as pct_missing_hole,
    ROUND(pct_low_battery, 1) as pct_low_battery
FROM iceberg.gold.data_quality_overview
ORDER BY data_quality_score DESC
"""

SQL_CRITICAL_GAPS = """
SELECT 
    course_id,
    total_events,
    total_rounds,
    ROUND(usability_score, 1) as usability_score,
    pace_data_status,
    location_data_status,
    device_health_status,
    round_config_status,
    top_recommendation
FROM iceberg.gold.critical_column_gaps
ORDER BY usability_score DESC
"""

SQL_COURSE_CONFIG = """
SELECT 
    course_id,
    total_rounds,
    likely_course_type,
    max_section_seen,
    max_holes_in_round,
    ROUND(pct_nine_hole, 1) as pct_nine_hole,
    unique_start_holes,
    ROUND(pct_shotgun_starts, 1) as pct_shotgun_starts,
    course_complexity_score
FROM iceberg.gold.course_configuration_analysis
ORDER BY course_complexity_score DESC
"""

SQL_BATTERY_BY_COURSE = """
SELECT 
    course_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) as low_battery,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_low_battery
FROM iceberg.silver.fact_telemetry_event
WHERE battery_percentage IS NOT NULL
GROUP BY course_id
ORDER BY pct_low_battery DESC
"""

SQL_PACE_GAP_BY_COURSE = """
SELECT 
    course_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) as missing_pace_gap,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_missing
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY pct_missing DESC
"""

SQL_COLUMN_COMPLETENESS = """
SELECT 
    course_id,
    COUNT(*) as total,
    ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_pct,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_gap_pct,
    ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as hole_pct,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as battery_pct,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as gps_pct
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY course_id
"""


# =============================================================================
# Page Components
# =============================================================================


def render_sidebar():
    """Render the sidebar navigation."""
    with st.sidebar:
        st.markdown("# ⛳ TagMarshal")
        st.caption("Data Quality Dashboard")

        st.divider()

        page = st.radio(
            "Navigate to",
            [
                "🏠 Executive Summary",
                "📊 Data Quality",
                "🏌️ Course Analysis",
                "⚠️ Critical Gaps",
            ],
            label_visibility="collapsed",
        )

        st.divider()

        st.markdown("### About")
        st.markdown(
            """
        This dashboard analyzes data from **5 pilot courses** 
        to assess readiness for full migration.
        
        **Data Source:** Trino (Iceberg tables)
        """
        )

        return page


def render_executive_summary():
    """Render the executive summary page."""
    st.title("⛳ TagMarshal Data Quality Report")
    st.markdown("### Executive Summary")

    # Key metrics
    show_sql(SQL_EXECUTIVE_SUMMARY, "Executive Summary Query")

    summary = run_query(SQL_EXECUTIVE_SUMMARY)

    if not summary.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Courses", f"{summary['total_courses'].iloc[0]:,}")
        with col2:
            st.metric("Total Rounds", f"{summary['total_rounds'].iloc[0]:,}")
        with col3:
            st.metric("Total Events", f"{summary['total_events'].iloc[0]:,}")

    st.divider()

    # Data quality overview
    st.markdown("### Data Quality by Course")

    show_sql(SQL_DATA_QUALITY_OVERVIEW, "Data Quality Overview Query")

    quality_df = run_query(SQL_DATA_QUALITY_OVERVIEW)

    if not quality_df.empty:
        # Create quality score chart
        fig = px.bar(
            quality_df,
            x="course_id",
            y="data_quality_score",
            color="data_quality_score",
            color_continuous_scale=["#ff5252", "#ffc107", "#64dd17", "#00c853"],
            title="Data Quality Score by Course",
            labels={"data_quality_score": "Quality Score (%)", "course_id": "Course"},
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        fig.add_hline(
            y=80, line_dash="dash", line_color="green", annotation_text="Good threshold"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Display table
        st.dataframe(
            quality_df.style.background_gradient(
                subset=["data_quality_score"], cmap="RdYlGn"
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # Key findings
    st.markdown("### 🔑 Key Findings")

    col1, col2 = st.columns(2)

    with col1:
        st.success(
            """
        **✅ Ready for Migration**
        - Pinehurst 4: 98.6% quality
        - Erin Hills: 98.2% quality
        - American Falls: 98.0% quality
        """
        )

    with col2:
        st.warning(
            """
        **⚠️ Needs Attention**
        - American Falls: 51.6% low battery events
        - Indian Creek: 100% missing pace_gap
        - Bradshaw Farm: 13.6% missing hole numbers
        """
        )


def render_data_quality():
    """Render the detailed data quality page."""
    st.title("📊 Data Quality Analysis")

    # Column completeness
    st.markdown("### Column Completeness by Course")

    show_sql(SQL_COLUMN_COMPLETENESS, "Column Completeness Query")

    completeness_df = run_query(SQL_COLUMN_COMPLETENESS)

    if not completeness_df.empty:
        # Reshape for heatmap
        heatmap_data = completeness_df.set_index("course_id")[
            ["pace_pct", "pace_gap_pct", "hole_pct", "battery_pct", "gps_pct"]
        ]
        heatmap_data.columns = ["Pace", "Pace Gap", "Hole #", "Battery", "GPS"]

        fig = px.imshow(
            heatmap_data,
            color_continuous_scale=["#ff5252", "#ffc107", "#64dd17", "#00c853"],
            aspect="auto",
            title="Column Completeness Heatmap (%)",
            labels=dict(x="Column", y="Course", color="% Complete"),
        )
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(completeness_df, use_container_width=True, hide_index=True)

    st.divider()

    # Battery analysis
    st.markdown("### 🔋 Battery Health Analysis")

    show_sql(SQL_BATTERY_BY_COURSE, "Battery Analysis Query")

    battery_df = run_query(SQL_BATTERY_BY_COURSE)

    if not battery_df.empty:
        fig = px.bar(
            battery_df,
            x="course_id",
            y="pct_low_battery",
            color="pct_low_battery",
            color_continuous_scale=["#00c853", "#ffc107", "#ff5252"],
            title="Low Battery Events by Course (%)",
            labels={"pct_low_battery": "% Low Battery (<20%)", "course_id": "Course"},
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        fig.add_hline(
            y=10,
            line_dash="dash",
            line_color="red",
            annotation_text="Warning threshold",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(battery_df, use_container_width=True, hide_index=True)

    st.divider()

    # Pace gap analysis
    st.markdown("### ⏱️ Pace Gap Coverage")

    show_sql(SQL_PACE_GAP_BY_COURSE, "Pace Gap Analysis Query")

    pace_df = run_query(SQL_PACE_GAP_BY_COURSE)

    if not pace_df.empty:
        fig = px.bar(
            pace_df,
            x="course_id",
            y="pct_missing",
            color="pct_missing",
            color_continuous_scale=["#00c853", "#ffc107", "#ff5252"],
            title="Missing Pace Gap Values by Course (%)",
            labels={"pct_missing": "% Missing", "course_id": "Course"},
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(pace_df, use_container_width=True, hide_index=True)


def render_course_analysis():
    """Render the course configuration analysis page."""
    st.title("🏌️ Course Configuration Analysis")

    st.markdown(
        """
    Understanding course complexity is critical for pace management. 
    This analysis shows the variety of course configurations in the pilot data.
    """
    )

    show_sql(SQL_COURSE_CONFIG, "Course Configuration Query")

    config_df = run_query(SQL_COURSE_CONFIG)

    if not config_df.empty:
        # Course types pie chart
        col1, col2 = st.columns(2)

        with col1:
            type_counts = config_df["likely_course_type"].value_counts()
            fig = px.pie(
                values=type_counts.values,
                names=type_counts.index,
                title="Course Types Distribution",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                config_df,
                x="course_id",
                y="course_complexity_score",
                color="likely_course_type",
                title="Course Complexity Score",
                labels={
                    "course_complexity_score": "Complexity Score",
                    "course_id": "Course",
                },
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Detailed table
        st.markdown("### Course Configuration Details")
        st.dataframe(config_df, use_container_width=True, hide_index=True)

        st.divider()

        # Key insights
        st.markdown("### 🔍 Key Insights")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.info(
                """
            **27-Hole Courses**
            - Bradshaw Farm
            - Erin Hills
            
            *Require nine_number tracking*
            """
            )

        with col2:
            st.info(
                """
            **Shotgun Starts**
            - Indian Creek (9 holes)
            - Pinehurst 4 (10 holes)
            
            *Require start_hole normalization*
            """
            )

        with col3:
            st.info(
                """
            **9-Hole with 18-Hole Rounds**
            - American Falls
            
            *47% replay for 18 holes*
            """
            )


def render_critical_gaps():
    """Render the critical gaps analysis page."""
    st.title("⚠️ Critical Data Gaps")

    st.markdown(
        """
    This analysis identifies critical data gaps that could impact analytics and reporting.
    Each course is evaluated across 4 tiers of data quality.
    """
    )

    show_sql(SQL_CRITICAL_GAPS, "Critical Gaps Query")

    gaps_df = run_query(SQL_CRITICAL_GAPS)

    if not gaps_df.empty:
        # Usability score chart
        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=gaps_df["course_id"],
                y=gaps_df["usability_score"],
                marker_color=gaps_df["usability_score"].apply(
                    lambda x: (
                        "#00c853"
                        if x >= 95
                        else (
                            "#64dd17"
                            if x >= 90
                            else "#ffc107" if x >= 80 else "#ff5252"
                        )
                    )
                ),
                text=gaps_df["usability_score"].apply(lambda x: f"{x}%"),
                textposition="outside",
            )
        )

        fig.update_layout(
            title="Usability Score by Course",
            xaxis_title="Course",
            yaxis_title="Usability Score (%)",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(range=[0, 105]),
        )

        fig.add_hline(
            y=80,
            line_dash="dash",
            line_color="green",
            annotation_text="Minimum acceptable",
        )

        st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Status by tier
        st.markdown("### Status by Data Tier")

        status_cols = [
            "pace_data_status",
            "location_data_status",
            "device_health_status",
            "round_config_status",
        ]
        tier_names = ["Pace Data", "Location Data", "Device Health", "Round Config"]

        for course in gaps_df["course_id"].unique():
            course_data = gaps_df[gaps_df["course_id"] == course].iloc[0]

            with st.expander(
                f"**{course}** - Usability: {course_data['usability_score']}%",
                expanded=False,
            ):
                cols = st.columns(4)

                for i, (col, tier, name) in enumerate(
                    zip(cols, status_cols, tier_names)
                ):
                    status = course_data[tier]
                    if status == "EXCELLENT":
                        col.success(f"**{name}**\n\n✅ {status}")
                    elif status == "GOOD":
                        col.success(f"**{name}**\n\n✅ {status}")
                    elif status == "FAIR":
                        col.warning(f"**{name}**\n\n⚠️ {status}")
                    else:
                        col.error(f"**{name}**\n\n❌ {status}")

                if course_data["top_recommendation"]:
                    st.markdown(
                        f"**💡 Recommendation:** {course_data['top_recommendation']}"
                    )

        st.divider()

        # Full table
        st.markdown("### Detailed Gap Analysis")
        st.dataframe(gaps_df, use_container_width=True, hide_index=True)

        st.divider()

        # Recommendations summary
        st.markdown("### 📋 Action Items")

        for _, row in gaps_df.iterrows():
            if row["top_recommendation"] and row["usability_score"] < 100:
                st.markdown(f"- **{row['course_id']}**: {row['top_recommendation']}")


# =============================================================================
# Main App
# =============================================================================


def render_connection_error():
    """Show connection error message."""
    st.error("## ⚠️ Cannot Connect to Trino")
    st.markdown(
        """
    The dashboard cannot connect to the Trino query engine.
    
    **To fix this:**
    
    1. **Start Docker Desktop** (if not running)
    2. **Start the lakehouse stack:**
       ```bash
       just up
       ```
    3. **Wait for Trino to be ready:**
       ```bash
       just trino-status
       ```
    4. **Refresh this page** once Trino shows "healthy"
    
    ---
    
    **Connection Details:**
    - Host: `localhost`
    - Port: `8080`
    - Catalog: `iceberg`
    """
    )


def main():
    """Main app entry point."""
    page = render_sidebar()

    # Check Trino connection once at the start
    if not check_trino_connection():
        render_connection_error()
        return

    if page == "🏠 Executive Summary":
        render_executive_summary()
    elif page == "📊 Data Quality":
        render_data_quality()
    elif page == "🏌️ Course Analysis":
        render_course_analysis()
    elif page == "⚠️ Critical Gaps":
        render_critical_gaps()


if __name__ == "__main__":
    main()
