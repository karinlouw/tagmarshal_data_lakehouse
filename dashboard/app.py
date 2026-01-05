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
    """Load SQL query from pipeline/queries/exploration directory."""
    query_path = (
        Path(__file__).parent.parent / "pipeline" / "queries" / "exploration" / filename
    )
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
SQL_BOTTLENECK_ANALYSIS = load_query("bottleneck_analysis.sql")


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
                "🗺️ Course Map",
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

    st.info(
        """
    **🔍 Data Audit Phase**: This page shows data completeness and null patterns.
    We've preserved null values in the Silver layer to understand data quality before transformation.
    """
    )

    # Data Completeness Overview (NEW)
    st.markdown("### 📋 Data completeness overview")

    completeness_query = """
    SELECT 
        course_id,
        COUNT(*) as total_records,
        COUNT(DISTINCT round_id) as unique_rounds,
        ROUND(100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL AND is_timestamp_missing = false THEN 1 ELSE 0 END) / COUNT(*), 1) as timestamp_pct,
        ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_pct,
        ROUND(100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_gap_pct,
        ROUND(100.0 * SUM(CASE WHEN positional_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pos_gap_pct,
        ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as hole_pct,
        ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as gps_pct,
        ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as battery_pct,
        ROUND(100.0 * SUM(CASE WHEN start_hole IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as start_hole_pct,
        ROUND(
            (
                100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL AND is_timestamp_missing = false THEN 1 ELSE 0 END) / COUNT(*) +
                100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) +
                100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) +
                100.0 * SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*)
            ) / 4, 1
        ) as completeness_score
    FROM iceberg.silver.fact_telemetry_event
    GROUP BY course_id
    ORDER BY completeness_score DESC
    """

    completeness_df = run_query(completeness_query)

    if not completeness_df.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        total_records = completeness_df["total_records"].sum()
        total_rounds = completeness_df["unique_rounds"].sum()
        avg_completeness = completeness_df["completeness_score"].mean()

        with col1:
            st.metric("Total records", f"{total_records:,}")
        with col2:
            st.metric("Total rounds", f"{total_rounds:,}")
        with col3:
            st.metric("Avg completeness", f"{avg_completeness:.1f}%")
        with col4:
            # Count courses with >80% completeness
            good_courses = len(
                completeness_df[completeness_df["completeness_score"] >= 80]
            )
            st.metric("Courses ≥80% complete", f"{good_courses}/{len(completeness_df)}")

        # Completeness score bar chart
        fig = px.bar(
            completeness_df,
            x="course_id",
            y="completeness_score",
            color="completeness_score",
            color_continuous_scale=["#ff5252", "#ffc107", "#00c853"],
            title="Overall data completeness score by course",
            labels={"completeness_score": "Completeness (%)", "course_id": "Course"},
        )
        fig.add_hline(
            y=80, line_dash="dash", line_color="green", annotation_text="Target: 80%"
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Traffic light status per course
        st.markdown("#### Status by course")

        for _, row in completeness_df.iterrows():
            score = row["completeness_score"]
            records = row["total_records"]
            rounds = row["unique_rounds"]

            if score >= 90:
                status = "🟢"
                label = "Excellent"
            elif score >= 80:
                status = "🟡"
                label = "Good"
            elif score >= 60:
                status = "🟠"
                label = "Fair"
            else:
                status = "🔴"
                label = "Poor"

            with st.expander(
                f"{status} **{row['course_id']}** - {score:.1f}% ({label})"
            ):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**Records:** {records:,}")
                    st.markdown(f"**Rounds:** {rounds:,}")
                with col2:
                    st.markdown(f"**Timestamp:** {row['timestamp_pct']:.1f}%")
                    st.markdown(f"**Pace:** {row['pace_pct']:.1f}%")
                    st.markdown(f"**Pace gap:** {row['pace_gap_pct']:.1f}%")
                    st.markdown(f"**Hole #:** {row['hole_pct']:.1f}%")
                    st.markdown(f"**GPS:** {row['gps_pct']:.1f}%")
                    st.markdown(f"**Battery:** {row['battery_pct']:.1f}%")
                    st.markdown(f"**Start hole:** {row['start_hole_pct']:.1f}%")

        # Detailed completeness table
        st.markdown("#### Detailed completeness table")
        display_df = completeness_df.copy()
        display_df.columns = [
            "Course",
            "Records",
            "Rounds",
            "Timestamp %",
            "Pace %",
            "Pace gap %",
            "Pos gap %",
            "Hole %",
            "GPS %",
            "Battery %",
            "Start hole %",
            "Score",
        ]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        show_sql(completeness_query, "Data completeness query")

    st.divider()

    # Null records analysis - show actual null counts
    st.markdown("### 🚫 Null value breakdown")
    st.markdown("Raw counts of null values per column (preserved for audit).")

    null_counts_query = """
    SELECT 
        course_id,
        COUNT(*) as total_records,
        SUM(CASE WHEN is_timestamp_missing = true THEN 1 ELSE 0 END) as null_timestamp,
        SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) as null_pace,
        SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) as null_pace_gap,
        SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) as null_pos_gap,
        SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) as null_hole,
        SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) as null_section,
        SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) as null_gps,
        SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) as null_battery,
        SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) as null_start_hole,
        SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) as null_goal_time
    FROM iceberg.silver.fact_telemetry_event
    GROUP BY course_id
    ORDER BY course_id
    """

    null_counts_df = run_query(null_counts_query)

    if not null_counts_df.empty:
        # Total null counts across all courses
        total_nulls = {
            "Timestamp": null_counts_df["null_timestamp"].sum(),
            "Pace": null_counts_df["null_pace"].sum(),
            "Pace gap": null_counts_df["null_pace_gap"].sum(),
            "Pos gap": null_counts_df["null_pos_gap"].sum(),
            "Hole #": null_counts_df["null_hole"].sum(),
            "Section": null_counts_df["null_section"].sum(),
            "GPS": null_counts_df["null_gps"].sum(),
            "Battery": null_counts_df["null_battery"].sum(),
            "Start hole": null_counts_df["null_start_hole"].sum(),
        }

        # Bar chart of total nulls
        null_chart_df = pd.DataFrame(
            {
                "Column": list(total_nulls.keys()),
                "Null count": list(total_nulls.values()),
            }
        )

        fig = px.bar(
            null_chart_df,
            x="Column",
            y="Null count",
            color="Null count",
            color_continuous_scale=["#00c853", "#ffc107", "#ff5252"],
            title="Total null values by column (all courses)",
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Key insights
        st.markdown("#### 🎯 Key findings")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Columns with most nulls:**")
            sorted_nulls = sorted(total_nulls.items(), key=lambda x: x[1], reverse=True)
            for col_name, count in sorted_nulls[:5]:
                if count > 0:
                    total = null_counts_df["total_records"].sum()
                    pct = 100.0 * count / total
                    status = "🔴" if pct > 20 else "🟠" if pct > 5 else "🟢"
                    st.markdown(f"- {status} **{col_name}**: {count:,} ({pct:.1f}%)")

        with col2:
            st.markdown("**Impact assessment:**")
            # Pace gap is critical for bottleneck analysis
            pace_gap_nulls = total_nulls["Pace gap"]
            total = null_counts_df["total_records"].sum()
            pace_gap_pct = 100.0 * pace_gap_nulls / total

            if pace_gap_pct > 50:
                st.error(
                    f"⚠️ {pace_gap_pct:.0f}% of records missing pace_gap - bottleneck analysis limited"
                )
            elif pace_gap_pct > 20:
                st.warning(
                    f"⚠️ {pace_gap_pct:.0f}% missing pace_gap - some bottleneck analysis possible"
                )
            else:
                st.success(
                    f"✅ Only {pace_gap_pct:.0f}% missing pace_gap - good for bottleneck analysis"
                )

            # GPS completeness
            gps_nulls = total_nulls["GPS"]
            gps_pct = 100.0 * gps_nulls / total
            if gps_pct > 5:
                st.warning(f"⚠️ {gps_pct:.1f}% missing GPS - affects mapping")
            else:
                st.success(f"✅ Only {gps_pct:.1f}% missing GPS - good for mapping")

        # Raw null counts table
        st.markdown("#### Raw null counts by course")
        display_df = null_counts_df.copy()
        display_df.columns = [
            "Course",
            "Total",
            "Timestamp",
            "Pace",
            "Pace gap",
            "Pos gap",
            "Hole",
            "Section",
            "GPS",
            "Battery",
            "Start hole",
            "Goal time",
        ]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        show_sql(null_counts_query, "Null counts query")

    st.divider()

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


def render_course_map():
    """
    Render the course map page showing bottleneck locations.

    Uses pace_gap (time to group ahead) instead of pace (time vs goal).
    Why? pace_gap shows actual group spacing - a spike indicates a bottleneck.
    pace is relative to arbitrary goal times set by courses.

    Key columns:
    - pace_gap: Time gap to group ahead (seconds). High variance = bottleneck
    - positional_gap: Position relative to group ahead. Positive = falling behind
    - latitude, longitude: GPS coordinates for mapping
    """
    st.title("🗺️ Course Bottleneck Map")

    st.markdown(
        """
    This map shows where groups bunch up on each course using **pace gap** analysis.
    
    **Why pace gap?** It measures actual spacing between groups, not arbitrary goal times.
    - **Red markers** = High variance in spacing (groups bunching up = bottleneck)
    - **Green markers** = Consistent spacing (smooth flow)
    """
    )

    # Get bottleneck data
    bottleneck_df = run_query(SQL_BOTTLENECK_ANALYSIS)

    if bottleneck_df.empty:
        st.warning("No pace gap data available. This course may not have gap metrics.")
        return

    # Course selector - only show courses with pace_gap data
    courses = sorted(bottleneck_df["course_id"].unique())

    if not courses:
        st.warning("No courses have pace gap data for bottleneck analysis.")
        return

    selected_course = st.selectbox("Select course", courses)

    # Filter to selected course
    course_df = bottleneck_df[bottleneck_df["course_id"] == selected_course].copy()

    if course_df.empty:
        st.warning(f"No pace gap data for {selected_course}")
        return

    st.divider()

    # Metric selector
    st.markdown("### Select bottleneck metric")

    metric_options = {
        "Pace gap variance (std dev)": {
            "column": "pace_gap_stddev",
            "label": "Pace gap std dev",
            "description": "Variance in spacing between groups. **High = inconsistent spacing = bottleneck**",
            "color_scale": ["#00c853", "#ffc107", "#ff5252"],  # Green to red
            "sort_ascending": False,  # High is bad
        },
        "Average pace gap": {
            "column": "avg_pace_gap_seconds",
            "label": "Avg pace gap (sec)",
            "description": "Average seconds between you and group ahead. **High = spread out, Low = tight spacing**",
            "color_scale": ["#00c853", "#ffc107", "#ff5252"],  # Green to red
            "sort_ascending": False,  # High spacing is generally better
        },
        "Positional gap": {
            "column": "avg_positional_gap",
            "label": "Avg positional gap",
            "description": "Are groups catching up or falling behind? **Positive = falling behind = problem**",
            "color_scale": ["#00c853", "#ffc107", "#ff5252"],  # Green to red
            "sort_ascending": False,  # Positive is bad
        },
        "Traditional pace": {
            "column": "avg_pace_seconds",
            "label": "Avg pace (sec)",
            "description": "Seconds behind/ahead of goal time. **Positive = behind schedule** (less reliable - goal times are arbitrary)",
            "color_scale": ["#00c853", "#ffc107", "#ff5252"],  # Green to red
            "sort_ascending": False,  # High is bad
        },
    }

    selected_metric = st.radio(
        "Choose metric to analyse",
        options=list(metric_options.keys()),
        horizontal=True,
        help="Different metrics reveal different aspects of bottleneck formation",
    )

    metric_config = metric_options[selected_metric]
    metric_column = metric_config["column"]

    # Show description
    st.info(f"**{selected_metric}**: {metric_config['description']}")

    st.divider()

    # Explain all metrics
    with st.expander("📖 Understanding all metrics"):
        st.markdown(
            """
        | Metric | What it means | What it reveals |
        |--------|---------------|-----------------|
        | **Pace gap variance** | Standard deviation of spacing between groups | **Best for bottlenecks** - High variance = groups bunching up |
        | **Average pace gap** | Mean seconds between you and group ahead | Shows if course is generally spread out or tight |
        | **Positional gap** | Are you catching up or falling behind? | Identifies sections where groups consistently fall behind |
        | **Traditional pace** | Seconds behind/ahead of goal time | Less reliable - goal times are course-set and often unrealistic |
        
        **Recommendation:** Start with **Pace gap variance** - it's the best indicator of actual bottlenecks.
        """
        )

    # Create map - colour by selected metric
    st.markdown(f"### Section locations by {selected_metric.lower()}")

    # Prepare hover data with all metrics
    hover_data = {
        "section_number": True,
        "hole_section": True,
        "rounds_measured": ":,",
    }
    # Add all metric columns to hover
    for metric_name, config in metric_options.items():
        hover_data[config["column"]] = ":.0f"

    fig = px.scatter_mapbox(
        course_df,
        lat="lat",
        lon="lon",
        color=metric_column,
        size="rounds_measured",
        hover_name="hole_number",
        hover_data=hover_data,
        color_continuous_scale=metric_config["color_scale"],
        labels={
            metric_column: metric_config["label"],
            "rounds_measured": "Rounds",
            "section_number": "Section",
            "hole_section": "Hole section",
        },
        title=f"Bottleneck analysis: {selected_course} ({selected_metric})",
        zoom=15,
    )

    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        height=500,
    )

    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Bottleneck summary - sort by selected metric
    st.markdown("### Bottleneck summary")

    # Aggregate by hole for summary
    agg_dict = {
        metric_column: "mean",
        "rounds_measured": "sum",
    }
    # Add positional gap if not already selected
    if metric_column != "avg_positional_gap":
        agg_dict["avg_positional_gap"] = "mean"

    hole_summary = (
        course_df.groupby("hole_number")
        .agg(agg_dict)
        .reset_index()
        .sort_values(metric_column, ascending=metric_config["sort_ascending"])
    )

    col1, col2 = st.columns(2)

    with col1:
        # Worst holes (highest values for most metrics, but depends on metric)
        if metric_config["sort_ascending"]:
            label = "🟢 Best (lowest values)"
            worst = hole_summary.head(3)
        else:
            label = "🔴 Worst (highest values)"
            worst = hole_summary.head(3)

        st.markdown(f"**{label}**")
        for _, row in worst.iterrows():
            value = row[metric_column]
            hole = int(row["hole_number"])

            # Format based on metric type
            if "stddev" in metric_column:
                st.markdown(f"- **Hole {hole}**: σ={value:.0f}s")
            elif "gap" in metric_column or "pace" in metric_column:
                mins = abs(value) / 60
                sign = "+" if value > 0 else ""
                st.markdown(f"- **Hole {hole}**: {sign}{value:.0f}s ({mins:.1f} min)")
            else:
                st.markdown(f"- **Hole {hole}**: {value:.0f}")

    with col2:
        # Best holes (opposite of worst)
        if metric_config["sort_ascending"]:
            label = "🔴 Worst (highest values)"
            best = hole_summary.tail(3).iloc[::-1]
        else:
            label = "🟢 Best (lowest values)"
            best = hole_summary.tail(3).iloc[::-1]

        st.markdown(f"**{label}**")
        for _, row in best.iterrows():
            value = row[metric_column]
            hole = int(row["hole_number"])

            # Format based on metric type
            if "stddev" in metric_column:
                st.markdown(f"- **Hole {hole}**: σ={value:.0f}s")
            elif "gap" in metric_column or "pace" in metric_column:
                mins = abs(value) / 60
                sign = "+" if value > 0 else ""
                st.markdown(f"- **Hole {hole}**: {sign}{value:.0f}s ({mins:.1f} min)")
            else:
                st.markdown(f"- **Hole {hole}**: {value:.0f}")

    st.divider()

    # Data table with all metrics
    st.markdown("### Detailed data by section")
    st.caption(f"💡 Currently viewing: **{metric_config['label']}** (marked with ⭐)")

    # Build column mapping
    column_mapping = {
        "hole_number": "Hole",
        "section_number": "Section",
        "hole_section": "Hole sec",
        "pace_gap_stddev": "Gap std dev",
        "avg_pace_gap_seconds": "Avg gap (sec)",
        "avg_positional_gap": "Pos gap",
        "avg_pace_seconds": "Traditional pace",
        "rounds_measured": "Rounds",
    }

    # Mark selected metric with star
    column_mapping[metric_column] = metric_config["label"] + " ⭐"

    # Select and rename columns
    display_df = (
        course_df[
            [
                "hole_number",
                "section_number",
                "hole_section",
                "pace_gap_stddev",
                "avg_pace_gap_seconds",
                "avg_positional_gap",
                "avg_pace_seconds",
                "rounds_measured",
            ]
        ]
        .sort_values("section_number")
        .copy()
    )

    # Reorder to put selected metric after location columns
    base_cols = ["hole_number", "section_number", "hole_section"]
    metric_cols = [metric_column]
    other_cols = [c for c in display_df.columns if c not in base_cols + metric_cols]
    column_order = base_cols + metric_cols + other_cols

    display_df = display_df[column_order]
    display_df = display_df.rename(columns=column_mapping)

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    show_sql(SQL_BOTTLENECK_ANALYSIS, "Bottleneck analysis query")


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
    elif page == "🗺️ Course Map":
        render_course_map()
    elif page == "⚠️ Critical Gaps":
        render_critical_gaps()


if __name__ == "__main__":
    main()
