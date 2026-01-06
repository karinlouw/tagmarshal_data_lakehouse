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


def load_example_query(filename: str) -> str:
    """Load SQL query from pipeline/queries/examples directory."""
    query_path = (
        Path(__file__).parent.parent / "pipeline" / "queries" / "examples" / filename
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

# Example queries (documentation-grade)
SQL_EXAMPLE_LOOP_FATIGUE = load_example_query("check_loop_fatigue.sql")
SQL_EXAMPLE_SHOTGUN_STARTS = load_example_query(
    "indiancreek_shotgun_start_distribution.sql"
)


# =============================================================================
# Page Components
# =============================================================================


def get_all_courses():
    """Get list of all available courses."""
    query = "SELECT DISTINCT course_id FROM iceberg.silver.fact_telemetry_event ORDER BY course_id"
    try:
        df = run_query(query)
        if not df.empty:
            return df["course_id"].tolist()
        return []
    except Exception:
        return []


def render_sidebar():
    """Render the sidebar navigation."""
    with st.sidebar:
        st.markdown("## ⛳ TagMarshal")
        st.caption("Lakehouse Intelligence Dashboard")

        st.divider()

        st.markdown("### 🧭 Navigation")

        page = st.selectbox(
            "Select a view",
            [
                "🏠 Summary",
                "📊 Data quality (gold)",
                "⚠️ Critical gaps (gold)",
                "🔋 Device health (gold)",
                "🏌️ Course analysis",
                "🗺️ Bottleneck map",
            ],
            index=0,
            key="nav_select",
        )

        st.divider()

        # Filters
        st.markdown("### 🔍 Filters")

        # Course filter
        all_courses = get_all_courses()
        selected_courses = []

        if all_courses:
            selected_courses = st.multiselect(
                "Filter by Course",
                options=all_courses,
                default=all_courses,
                help="Select courses to include in the analysis",
            )

            if not selected_courses:
                st.warning("Please select at least one course.")

        st.divider()

        st.info(
            """
            **Data Freshness**
        
            Gold models updated: *Daily*
            Silver ingestion: *Hourly*
        """
        )

        return page, selected_courses


def render_executive_summary(selected_courses=None):
    """Render the executive summary page."""
    st.title("⛳ TagMarshal data quality report")
    st.markdown("### Summary")

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


def render_data_quality(selected_courses=None):
    """Render the detailed data quality page using Gold models."""
    st.title("📊 Data quality analysis (gold layer)")

    st.markdown(
        """
    This view uses the **Gold Layer** `data_quality_overview` model, which provides 
    pre-calculated completeness metrics for all critical columns.
    """
    )

    if selected_courses is not None and len(selected_courses) == 0:
        st.warning("Please select at least one course in the sidebar to view data.")
        return

    # Query Gold Model
    dq_query = """
    SELECT * FROM iceberg.gold.data_quality_overview
    ORDER BY data_quality_score DESC
    """

    dq_df = run_query(dq_query)

    if dq_df.empty:
        st.error(
            "No data found in iceberg.gold.data_quality_overview. Has the dbt model run?"
        )
        return

    # Filter in Python
    if selected_courses:
        dq_df = dq_df[dq_df["course_id"].isin(selected_courses)]

    if dq_df.empty:
        st.warning("No data found for selected courses.")
        return

    # --- KPI Row ---
    col1, col2, col3, col4 = st.columns(4)

    avg_score = dq_df["data_quality_score"].mean()
    total_records = dq_df["total_events"].sum()
    total_rounds = dq_df["total_rounds"].sum()
    perfect_courses = len(dq_df[dq_df["data_quality_score"] >= 95])

    col1.metric("Avg quality score", f"{avg_score:.2f}%", delta_color="normal")
    col2.metric("Total events analyzed", f"{total_records:,}")
    col3.metric("Total rounds", f"{total_rounds:,}")
    col4.metric("High quality courses", f"{perfect_courses}/{len(dq_df)}")

    st.divider()

    tab1, tab2, tab3 = st.tabs(["Overview", "Detailed metrics", "Raw data"])

    with tab1:
        st.subheader("Data quality score by course")

        fig = px.bar(
            dq_df,
            x="course_id",
            y="data_quality_score",
            color="data_quality_score",
            color_continuous_scale=[
                "#ff5252",
                "#ffc107",
                "#00c853",
            ],  # Red -> Yellow -> Green
            title="Overall data quality score",
            labels={"data_quality_score": "Quality score (%)", "course_id": "Course"},
        )
        fig.add_hline(
            y=90, line_dash="dash", line_color="green", annotation_text="Target (90%)"
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### 🚨 Critical issues")

        # Identify courses with specific problems
        problems = []
        for _, row in dq_df.iterrows():
            cid = row["course_id"]
            if row["pct_missing_pace"] > 10:
                problems.append(
                    f"**{cid}**: {row['pct_missing_pace']:.2f}% missing Pace data"
                )
            if row["pct_missing_hole"] > 5:
                problems.append(
                    f"**{cid}**: {row['pct_missing_hole']:.2f}% missing Hole numbers"
                )
            if row["pct_low_battery"] > 20:
                problems.append(
                    f"**{cid}**: {row['pct_low_battery']:.2f}% Low battery events"
                )

        if problems:
            for p in problems:
                st.warning(p, icon="⚠️")
        else:
            st.success(
                "No critical data quality issues detected across selected courses!",
                icon="✅",
            )

    with tab2:
        st.subheader("Missing data heatmap")
        st.caption("Percentage of null values per column (Lower is better)")

        # Select pct_missing columns
        pct_cols = [c for c in dq_df.columns if c.startswith("pct_missing_")]

        if pct_cols:
            heatmap_data = dq_df.set_index("course_id")[pct_cols]
            # Clean column names for display
            heatmap_data.columns = [
                c.replace("pct_missing_", "").replace("_", " ").title()
                for c in heatmap_data.columns
            ]

            fig = px.imshow(
                heatmap_data,
                color_continuous_scale=[
                    "#00c853",
                    "#ffc107",
                    "#ff5252",
                ],  # Green (0%) -> Red (100%)
                title="Missing data percentage",
                labels=dict(x="Metric", y="Course", color="% Missing"),
                text_auto=".2f",
            )
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.dataframe(dq_df, use_container_width=True, hide_index=True)
        show_sql(dq_query, "Source Query (Gold Model)")


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

        st.divider()

        # Seasonality Analysis
        st.markdown("### 📅 Seasonality analysis")
        st.markdown(
            """
            Analysis of round volumes by month and day of week to identify peak periods.
            """
        )

        monthly_sql = """
        SELECT 
            course_id,
            month_start,
            month_name,
            rounds,
            pct_total
        FROM iceberg.gold.course_rounds_by_month
        ORDER BY course_id, month_start
        """

        # Load data
        seasonality_df = run_query(monthly_sql)

        if not seasonality_df.empty:
            # Course selector
            courses = sorted(seasonality_df["course_id"].unique())
            selected_course_season = st.selectbox(
                "Select course for seasonality", courses, key="seasonality_course"
            )

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Monthly volume")
                course_monthly = seasonality_df[
                    seasonality_df["course_id"] == selected_course_season
                ]

                if not course_monthly.empty:
                    fig = px.bar(
                        course_monthly,
                        x="month_name",
                        y="rounds",
                        text="pct_total",
                        title=f"{selected_course_season}: Monthly rounds",
                        labels={
                            "rounds": "Rounds",
                            "month_name": "Month",
                            "pct_total": "% of Total",
                        },
                        color="rounds",
                        color_continuous_scale="Blues",
                    )
                    fig.update_traces(
                        texttemplate="%{text:.1f}%", textposition="outside"
                    )
                    fig.update_layout(
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        showlegend=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Weekly patterns")
                weekday_sql = """
                SELECT 
                    course_id,
                    weekday_number,
                    weekday_name,
                    rounds
                FROM iceberg.gold.course_rounds_by_weekday
                ORDER BY course_id, weekday_number
                """
                weekday_df = run_query(weekday_sql)
                course_weekday = weekday_df[
                    weekday_df["course_id"] == selected_course_season
                ]

                if not course_weekday.empty:
                    # Calculate percentage for weekday
                    total_rounds = course_weekday["rounds"].sum()
                    course_weekday["pct"] = (
                        course_weekday["rounds"] / total_rounds
                    ) * 100

                    fig = px.bar(
                        course_weekday,
                        x="weekday_name",
                        y="rounds",
                        text="pct",
                        title=f"{selected_course_season}: Rounds by day of week",
                        labels={"rounds": "Rounds", "weekday_name": "Day", "pct": "%"},
                        color="rounds",
                        color_continuous_scale="Greens",
                    )
                    fig.update_traces(
                        texttemplate="%{text:.1f}%", textposition="outside"
                    )
                    fig.update_layout(
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        showlegend=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("📋 View seasonality queries"):
                st.code(monthly_sql, language="sql")
                st.code(weekday_sql, language="sql")

        st.divider()

        st.markdown("### 🧭 Topology & routing insights (pilot examples)")
        st.caption(
            "These panels show how the topology-first model differentiates 27-hole units, 9-hole loops, and shotgun starts."
        )

        tab1, tab2, tab3 = st.tabs(
            [
                "Bradshaw (27-hole units)",
                "American Falls (loop fatigue)",
                "Indian Creek (shotgun starts)",
            ]
        )

        with tab1:
            st.markdown("#### Bradshaw Farm (27-hole) — unit breakdown")
            st.caption(
                "Compare the three 9-hole units via `nine_number` (derived from topology)."
            )

            bradshaw_topology_sql = """
            SELECT facility_id, unit_id, unit_name, nine_number, section_start, section_end
            FROM iceberg.silver.dim_facility_topology
            WHERE facility_id = 'bradshawfarmgc'
            ORDER BY nine_number
            """
            topo_df = run_query(bradshaw_topology_sql)
            if not topo_df.empty:
                st.dataframe(topo_df, use_container_width=True, hide_index=True)
            show_sql(bradshaw_topology_sql, "Bradshaw topology rows (Silver dim)")

            bradshaw_summary_sql = """
            SELECT
              nine_number,
              COUNT(DISTINCT round_id) AS rounds,
              AVG(pace) AS avg_pace_sec,
              AVG(pace_gap) AS avg_pace_gap_sec
            FROM iceberg.silver.fact_telemetry_event
            WHERE course_id = 'bradshawfarmgc'
              AND nine_number IS NOT NULL
            GROUP BY nine_number
            ORDER BY nine_number
            """
            br_df = run_query(bradshaw_summary_sql)
            if not br_df.empty:
                fig = px.bar(
                    br_df,
                    x="nine_number",
                    y="avg_pace_sec",
                    title="Bradshaw: average pace by nine_number (event-level)",
                    labels={
                        "nine_number": "Unit / Nine",
                        "avg_pace_sec": "Avg pace (sec)",
                    },
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(br_df, use_container_width=True, hide_index=True)
            show_sql(bradshaw_summary_sql, "Bradshaw unit summary (Silver)")

        with tab2:
            st.markdown(
                "#### American Falls — loop comparison on the same physical hole"
            )
            st.caption(
                "Because the same 9 holes are played twice, we group by `nine_number` to compare Loop 1 vs Loop 2."
            )

            hole = st.number_input(
                "Hole number", min_value=1, max_value=18, value=5, step=1
            )

            americanfalls_sql = f"""
            SELECT
              nine_number,
              AVG(avg_pace_sec) AS avg_pace_sec,
              COUNT(*) AS hole_instances
            FROM iceberg.gold.fact_round_hole_performance
            WHERE course_id = 'americanfalls'
              AND hole_number = {int(hole)}
            GROUP BY nine_number
            ORDER BY nine_number
            """
            af_df = run_query(americanfalls_sql)
            if not af_df.empty:
                fig = px.bar(
                    af_df,
                    x="nine_number",
                    y="avg_pace_sec",
                    title=f"American Falls: Hole {int(hole)} avg pace by loop (Gold)",
                    labels={
                        "nine_number": "Loop (nine_number)",
                        "avg_pace_sec": "Avg pace (sec)",
                    },
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(af_df, use_container_width=True, hide_index=True)
            show_sql(americanfalls_sql, "American Falls loop fatigue (Gold)")

            with st.expander("📌 Saved example query"):
                st.code(SQL_EXAMPLE_LOOP_FATIGUE, language="sql")

        with tab3:
            st.markdown("#### Indian Creek — shotgun start validation")
            st.caption(
                "Validate that rounds start on multiple holes via `start_hole` distribution."
            )

            indiancreek_sql = """
            SELECT
              start_hole,
              COUNT(DISTINCT round_id) AS rounds
            FROM iceberg.silver.fact_telemetry_event
            WHERE course_id = 'indiancreek'
              AND start_hole IS NOT NULL
            GROUP BY start_hole
            ORDER BY rounds DESC, start_hole
            """
            ic_df = run_query(indiancreek_sql)
            if not ic_df.empty:
                fig = px.bar(
                    ic_df,
                    x="start_hole",
                    y="rounds",
                    title="Indian Creek: rounds by start_hole (Silver)",
                    labels={"start_hole": "Start hole", "rounds": "Rounds"},
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(ic_df, use_container_width=True, hide_index=True)
            show_sql(
                indiancreek_sql, "Indian Creek shotgun start distribution (Silver)"
            )

            with st.expander("📌 Saved example query"):
                st.code(SQL_EXAMPLE_SHOTGUN_STARTS, language="sql")

    show_sql(SQL_COURSE_CONFIG, "Course configuration query")


def render_critical_gaps():
    """Render the critical gaps analysis page using Gold models."""
    st.title("⚠️ Critical data gaps (gold layer)")

    st.markdown(
        """
    This analysis identifies critical data gaps that could impact analytics and reporting.
    It uses the **Gold Layer** `critical_column_gaps` model, which evaluates each course across 4 tiers of data quality.
    """
    )

    with st.expander("ℹ️ How is the usability score calculated?"):
        st.markdown(
            """
        The **Usability score (0-100)** is a weighted index that penalizes missing values in critical columns.
        It does not weight all columns equally; instead, it prioritizes fields required for the primary use case (Pace of play).

        **Formula weights:**
        - **40% Pace management (Tier 1):** Checks the *worst case* of `pace` or `pace_gap` (avoids double-counting).
        - **30% Location tracking (Tier 2):** Checks `hole_number` and `fix_timestamp`.
        - **20% Device health (Tier 3):** Checks `battery_percentage` AND `is_projected` (signal quality).
        - **10% Configuration (Tier 4):** Checks `goal_time` AND `start_hole`.
        """
        )

    # Query Gold Model
    gaps_query = """
    SELECT * FROM iceberg.gold.critical_column_gaps
    ORDER BY usability_score DESC
    """

    gaps_df = run_query(gaps_query)

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
                text=gaps_df["usability_score"].apply(lambda x: f"{x:.2f}%"),
                textposition="outside",
            )
        )

        fig.update_layout(
            title="Usability score by course",
            xaxis_title="Course",
            yaxis_title="Usability score (%)",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(range=[0, 115]),  # Added space for text
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
        tier_names = [
            "Pace data (Tier 1)",
            "Location data (Tier 2)",
            "Device health (Tier 3)",
            "Round config (Tier 4)",
        ]

        for _, row in gaps_df.iterrows():
            course = row["course_id"]
            score = row["usability_score"]

            with st.expander(
                f"**{course}** - Usability: {score:.2f}%",
                expanded=False,
            ):
                cols = st.columns(4)

                for i, (col, db_col, name) in enumerate(
                    zip(cols, status_cols, tier_names)
                ):
                    status = row[db_col]
                    # Status already contains emoji from SQL
                    if "GOOD" in str(status) or "EXCELLENT" in str(status):
                        col.success(f"**{name}**\n\n{status}")
                    elif "WARNING" in str(status) or "FAIR" in str(status):
                        col.warning(f"**{name}**\n\n{status}")
                    else:
                        col.error(f"**{name}**\n\n{status}")

                if row["top_recommendation"]:
                    st.info(f"**💡 Recommendation:** {row['top_recommendation']}")

        st.divider()

        # Full table
        st.markdown("### Detailed Gap Analysis Table")
        st.dataframe(gaps_df, use_container_width=True, hide_index=True)

        # SQL query display
        show_sql(gaps_query, "Source Query (Gold Model)")

        st.divider()

        # Recommendations summary
        st.markdown("### 📋 Action items summary")

        for _, row in gaps_df.iterrows():
            if row["top_recommendation"] and row["usability_score"] < 100:
                st.markdown(f"- **{row['course_id']}**: {row['top_recommendation']}")
    else:
        st.error("No data found in iceberg.gold.critical_column_gaps.")


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


def render_device_health():
    """Render the device health analysis page using Gold models."""
    st.title("🔋 Device Health & Signal Quality (Gold Layer)")

    st.markdown(
        """
    Analysis of battery performance and signal reliability across the fleet.
    Uses **Gold Layer** models: `device_health_errors` and `signal_quality_rounds`.
    """
    )

    tab1, tab2 = st.tabs(["Battery Health", "Signal Quality"])

    with tab1:
        st.subheader("Battery Critical Events")

        # Query device_health_errors
        # Note: This model returns INDIVIDUAL events, so we might want to aggregate
        health_query = """
        SELECT 
            course_id,
            health_flag,
            COUNT(*) as event_count,
            COUNT(DISTINCT round_id) as affected_rounds
        FROM iceberg.gold.device_health_errors
        GROUP BY course_id, health_flag
        ORDER BY event_count DESC
        """

        health_df = run_query(health_query)

        if not health_df.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    health_df,
                    x="course_id",
                    y="event_count",
                    color="health_flag",
                    title="Battery Alerts by Course",
                    color_discrete_map={
                        "battery_critical": "#d32f2f",  # Dark Red
                        "battery_low": "#ff9800",  # Orange
                    },
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.dataframe(health_df, use_container_width=True)
        else:
            st.info("No battery health errors detected (Gold model returned no rows).")

    with tab2:
        st.subheader("Signal Quality & Projected Events")

        # Query signal_quality_rounds
        signal_query = """
        SELECT 
            course_id,
            AVG(projected_rate) * 100 as avg_projected_pct,
            AVG(problem_rate) * 100 as avg_problem_pct,
            COUNT(DISTINCT round_id) as total_rounds
        FROM iceberg.gold.signal_quality_rounds
        GROUP BY course_id
        ORDER BY avg_projected_pct DESC
        """

        signal_df = run_query(signal_query)

        if not signal_df.empty:
            st.markdown(
                "Higher **Projected %** indicates poor signal (GPS gaps filled by projection)."
            )

            fig = px.scatter(
                signal_df,
                x="avg_projected_pct",
                y="avg_problem_pct",
                size="total_rounds",
                color="course_id",
                text="course_id",
                title="Signal Quality Matrix (Size = Volume)",
                labels={
                    "avg_projected_pct": "Avg Projected Events (%)",
                    "avg_problem_pct": "Avg Problem Events (%)",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(signal_df, use_container_width=True)
        else:
            st.info("No signal quality data available.")


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
    page, selected_courses = render_sidebar()

    # Check Trino connection once at the start
    if not check_trino_connection():
        render_connection_error()
        return

    if page == "🏠 Summary":
        render_executive_summary(selected_courses)
    elif page == "📊 Data quality (gold)":
        render_data_quality(selected_courses)
    elif page == "⚠️ Critical gaps (gold)":
        render_critical_gaps()
    elif page == "🔋 Device health (gold)":
        render_device_health()
    elif page == "🏌️ Course analysis":
        render_course_analysis()
    elif page == "🗺️ Bottleneck map":
        render_course_map()


if __name__ == "__main__":
    main()
