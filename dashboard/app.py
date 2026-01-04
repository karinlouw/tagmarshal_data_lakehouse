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
from pathlib import Path
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
        port=8081,
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
# SQL Queries - Load from files
# =============================================================================


def load_query(filename: str) -> str:
    """Load SQL query from queries/exploration directory."""
    query_path = Path(__file__).parent.parent / "queries" / "exploration" / filename
    with open(query_path, "r") as f:
        return f.read().strip()


# Load all queries from files
SQL_EXECUTIVE_SUMMARY = load_query("executive_summary.sql")
SQL_DATA_QUALITY_OVERVIEW = load_query("data_quality_overview.sql")
SQL_CRITICAL_GAPS = load_query("critical_gaps.sql")
SQL_COURSE_CONFIG = load_query("course_configuration.sql")
SQL_BATTERY_BY_COURSE = load_query("battery_analysis.sql")
SQL_PACE_GAP_BY_COURSE = load_query("pace_gap_coverage.sql")
SQL_DATASET_VARIANCE = load_query("dataset_variance.sql")
SQL_NULL_ANALYSIS = load_query("null_analysis.sql")
SQL_COLUMN_COMPLETENESS = load_query("column_completeness.sql")


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
    st.markdown("### Executive summary")

    # Key metrics
    summary = run_query(SQL_EXECUTIVE_SUMMARY)

    if not summary.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total courses", f"{summary['total_courses'].iloc[0]:,}")
        with col2:
            st.metric("Total rounds", f"{summary['total_rounds'].iloc[0]:,}")
        with col3:
            st.metric("Total events", f"{summary['total_events'].iloc[0]:,}")

    show_sql(SQL_EXECUTIVE_SUMMARY, "Executive summary query")

    st.divider()

    # Data quality overview
    st.markdown("### Data quality by course")

    quality_df = run_query(SQL_DATA_QUALITY_OVERVIEW)

    if not quality_df.empty:
        # Create quality score chart
        fig = px.bar(
            quality_df,
            x="course_id",
            y="data_quality_score",
            color="data_quality_score",
            color_continuous_scale=["#ff5252", "#ffc107", "#64dd17", "#00c853"],
            title="Data quality score by course",
            labels={"data_quality_score": "Quality score (%)", "course_id": "Course"},
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
            quality_df,
            use_container_width=True,
            hide_index=True,
        )

    show_sql(SQL_DATA_QUALITY_OVERVIEW, "Data quality overview query")

    st.divider()

    # Key findings
    st.markdown("### 🔑 Key findings")

    col1, col2 = st.columns(2)

    with col1:
        st.success(
            """
        **✅ Ready for migration**
        - Pinehurst 4: 98.6% quality
        - Erin Hills: 98.2% quality
        - American Falls: 98.0% quality
        """
        )

    with col2:
        st.warning(
            """
        **⚠️ Needs attention**
        - American Falls: 51.6% low battery events
        - Indian Creek: 100% missing pace_gap
        - Bradshaw Farm: 13.6% missing hole numbers
        """
        )


def render_data_quality():
    """Render the detailed data quality page."""
    st.title("📊 Data quality analysis")

    # Column completeness
    st.markdown("### Column completeness by course")

    completeness_df = run_query(SQL_COLUMN_COMPLETENESS)

    if not completeness_df.empty:
        # Reshape for heatmap
        heatmap_data = completeness_df.set_index("course_id")[
            ["pace_pct", "pace_gap_pct", "hole_pct", "battery_pct", "gps_pct"]
        ]
        heatmap_data.columns = ["Pace", "Pace gap", "Hole #", "Battery", "GPS"]

        fig = px.imshow(
            heatmap_data,
            color_continuous_scale=["#ff5252", "#ffc107", "#64dd17", "#00c853"],
            aspect="auto",
            title="Column completeness heatmap (%)",
            labels=dict(x="Column", y="Course", color="% complete"),
        )
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(completeness_df, use_container_width=True, hide_index=True)

    show_sql(SQL_COLUMN_COMPLETENESS, "Column completeness query")

    st.divider()

    # Battery analysis
    st.markdown("### 🔋 Battery health analysis")

    battery_df = run_query(SQL_BATTERY_BY_COURSE)

    if not battery_df.empty:
        fig = px.bar(
            battery_df,
            x="course_id",
            y="pct_low_battery",
            color="pct_low_battery",
            color_continuous_scale=["#00c853", "#ffc107", "#ff5252"],
            title="Low battery events by course (%)",
            labels={"pct_low_battery": "% low battery (<20%)", "course_id": "Course"},
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

    show_sql(SQL_BATTERY_BY_COURSE, "Battery analysis query")

    st.divider()

    # Pace gap analysis
    st.markdown("### ⏱️ Pace gap coverage")

    pace_df = run_query(SQL_PACE_GAP_BY_COURSE)

    if not pace_df.empty:
        fig = px.bar(
            pace_df,
            x="course_id",
            y="pct_missing",
            color="pct_missing",
            color_continuous_scale=["#00c853", "#ffc107", "#ff5252"],
            title="Missing pace gap values by course (%)",
            labels={"pct_missing": "% missing", "course_id": "Course"},
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(pace_df, use_container_width=True, hide_index=True)

    show_sql(SQL_PACE_GAP_BY_COURSE, "Pace gap analysis query")

    st.divider()

    # Dataset variance analysis
    st.markdown("### 📊 Dataset variance analysis")
    st.markdown(
        "Understanding the differences in data volume and patterns across courses."
    )

    variance_df = run_query(SQL_DATASET_VARIANCE)

    if not variance_df.empty:
        # Events per round variance
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                variance_df,
                x="course_id",
                y="total_events",
                color="total_events",
                color_continuous_scale="Blues",
                title="Total events by course",
                labels={"total_events": "Events", "course_id": "Course"},
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                variance_df,
                x="course_id",
                y="avg_events_per_round",
                color="avg_events_per_round",
                color_continuous_scale="Greens",
                title="Average events per round",
                labels={
                    "avg_events_per_round": "Avg events/round",
                    "course_id": "Course",
                },
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        # Show variance metrics
        st.markdown("#### Key variance metrics")

        col1, col2, col3 = st.columns(3)
        with col1:
            min_events = variance_df["avg_events_per_round"].min()
            max_events = variance_df["avg_events_per_round"].max()
            st.metric(
                "Events per round range",
                f"{min_events:.0f} - {max_events:.0f}",
                delta=f"{max_events - min_events:.0f} variance",
            )
        with col2:
            max_loc = variance_df["max_location_index"].max()
            min_loc = variance_df["max_location_index"].min()
            st.metric(
                "Max location slots range",
                f"{min_loc} - {max_loc}",
                delta=f"{max_loc - min_loc} variance",
            )
        with col3:
            total_days = variance_df["unique_days"].sum()
            st.metric("Total unique days of data", f"{total_days}")

        st.dataframe(variance_df, use_container_width=True, hide_index=True)

    show_sql(SQL_DATASET_VARIANCE, "Dataset variance query")

    st.divider()

    # Null analysis
    st.markdown("### 🔍 Null value analysis")
    st.markdown("Detailed breakdown of missing data across all critical columns.")

    null_df = run_query(SQL_NULL_ANALYSIS)

    if not null_df.empty:
        # Create a heatmap of null percentages
        null_pct_cols = [col for col in null_df.columns if col.startswith("pct_null_")]

        if null_pct_cols:
            # Prepare data for heatmap
            heatmap_data = null_df[["course_id"] + null_pct_cols].copy()
            heatmap_data.columns = [
                (
                    col.replace("pct_null_", "").replace("_", " ").title()
                    if col != "course_id"
                    else col
                )
                for col in heatmap_data.columns
            ]

            # Melt for plotting
            heatmap_melted = heatmap_data.melt(
                id_vars=["course_id"], var_name="Column", value_name="% Null"
            )

            fig = px.density_heatmap(
                heatmap_melted,
                x="Column",
                y="course_id",
                z="% Null",
                color_continuous_scale=["#00c853", "#ffc107", "#ff5252"],
                title="Null percentage by column and course",
                labels={"course_id": "Course"},
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig, use_container_width=True)

        # Summary insights
        st.markdown("#### 🎯 Key null insights")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Columns with highest null rates:**")
            # Find columns with highest average null %
            avg_nulls = {}
            for col in null_pct_cols:
                avg_nulls[col.replace("pct_null_", "")] = null_df[col].mean()

            sorted_nulls = sorted(avg_nulls.items(), key=lambda x: x[1], reverse=True)
            for col_name, avg_pct in sorted_nulls[:5]:
                if avg_pct > 0:
                    status = "🔴" if avg_pct > 20 else "🟠" if avg_pct > 5 else "🟢"
                    st.markdown(f"- {status} **{col_name}**: {avg_pct:.1f}% average")

        with col2:
            st.markdown("**Courses with most missing data:**")
            # Calculate total null % per course
            null_df["total_null_pct"] = null_df[null_pct_cols].mean(axis=1)
            worst_courses = null_df.nlargest(3, "total_null_pct")
            for _, row in worst_courses.iterrows():
                status = (
                    "🔴"
                    if row["total_null_pct"] > 10
                    else "🟠" if row["total_null_pct"] > 5 else "🟢"
                )
                st.markdown(
                    f"- {status} **{row['course_id']}**: {row['total_null_pct']:.1f}% avg nulls"
                )

        # Full table (select relevant columns)
        display_cols = ["course_id", "total_rows"] + [
            col
            for col in null_df.columns
            if "null_" in col and not col.startswith("pct_")
        ]
        st.dataframe(
            (
                null_df[display_cols]
                if all(c in null_df.columns for c in display_cols)
                else null_df
            ),
            use_container_width=True,
            hide_index=True,
        )

    show_sql(SQL_NULL_ANALYSIS, "Null analysis query")


def render_course_analysis():
    """Render the course configuration analysis page."""
    st.title("🏌️ Course configuration analysis")

    st.markdown(
        """
    Understanding course complexity is critical for pace management. 
    This analysis shows the variety of course configurations in the pilot data.
    """
    )

    config_df = run_query(SQL_COURSE_CONFIG)

    if not config_df.empty:
        # Course types pie chart
        col1, col2 = st.columns(2)

        with col1:
            type_counts = config_df["likely_course_type"].value_counts()
            fig = px.pie(
                values=type_counts.values,
                names=type_counts.index,
                title="Course types distribution",
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
                title="Course complexity score",
                labels={
                    "course_complexity_score": "Complexity score",
                    "course_id": "Course",
                },
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Detailed table
        st.markdown("### Course configuration details")
        st.dataframe(config_df, use_container_width=True, hide_index=True)

        st.divider()

        # Key insights
        st.markdown("### 🔍 Key insights")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.info(
                """
            **27-hole courses**
            - Bradshaw Farm
            - Erin Hills
            
            *Require nine_number tracking*
            """
            )

        with col2:
            st.info(
                """
            **Shotgun starts**
            - Indian Creek (9 holes)
            - Pinehurst 4 (10 holes)
            
            *Require start_hole normalisation*
            """
            )

        with col3:
            st.info(
                """
            **9-hole with 18-hole rounds**
            - American Falls
            
            *47% replay for 18 holes*
            """
            )

    show_sql(SQL_COURSE_CONFIG, "Course configuration query")


def render_critical_gaps():
    """Render the critical gaps analysis page."""
    st.title("⚠️ Critical data gaps")

    st.markdown(
        """
    This analysis identifies critical data gaps that could impact analytics and reporting.
    Each course is evaluated across 4 tiers of data quality.
    """
    )

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
            title="Usability score by course",
            xaxis_title="Course",
            yaxis_title="Usability score (%)",
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
        st.markdown("### Status by data tier")

        status_cols = [
            "pace_data_status",
            "location_data_status",
            "device_health_status",
            "round_config_status",
        ]
        tier_names = ["Pace data", "Location data", "Device health", "Round config"]

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
                    # Status already contains emoji, just display it with correct background
                    if "GOOD" in str(status) or "EXCELLENT" in str(status):
                        col.success(f"**{name}**\n\n{status}")
                    elif "WARNING" in str(status) or "FAIR" in str(status):
                        col.warning(f"**{name}**\n\n{status}")
                    else:
                        col.error(f"**{name}**\n\n{status}")

                if course_data["top_recommendation"]:
                    st.markdown(
                        f"**💡 Recommendation:** {course_data['top_recommendation']}"
                    )

        st.divider()

        # Full table
        st.markdown("### Detailed gap analysis")
        st.dataframe(gaps_df, use_container_width=True, hide_index=True)

        # SQL query before action items
        show_sql(SQL_CRITICAL_GAPS, "Critical gaps query")

        st.divider()

        # Recommendations summary
        st.markdown("### 📋 Action items")

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
    - Port: `8081`
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
