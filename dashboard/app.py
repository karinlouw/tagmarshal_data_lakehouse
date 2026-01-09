"""Tagmarshal Lakehouse Data Explorer

A dashboard for exploring the pilot data and understanding the lakehouse
migration project. This tool is designed to walk stakeholders through
the data, its quality, and the possibilities of the full migration.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import textwrap
import traceback
from dataclasses import dataclass
from typing import Optional

from utils.database import execute_query, test_connection
from utils import queries
from utils.colors import (
    COLOR_GOOD,
    COLOR_WARNING,
    COLOR_CRITICAL,
    COLOR_INFO,
    COLOR_PRIMARY,
    COLOR_SECONDARY,
    COLOR_BORDER,
    COLOR_TEXT,
    COLOR_SCALE_QUALITY,
    COLOR_SEQUENCE,
)

# =============================================================================
# COURSE COLOR MAPPING (stable per course_id)
# =============================================================================


def get_course_color_map(course_ids: list[str]) -> dict[str, str]:
    """Return a stable course_id -> color mapping (deterministic).

    We sort course_ids to ensure stability across reruns, then assign colors from
    the centralized qualitative palette.
    """
    ids = sorted([str(c) for c in course_ids])
    if not ids:
        return {}
    return {cid: COLOR_SEQUENCE[i % len(COLOR_SEQUENCE)] for i, cid in enumerate(ids)}


# =============================================================================
# PAGE CONFIG
# =============================================================================

st.set_page_config(
    page_title="Tagmarshal data explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# STYLING
# =============================================================================

st.markdown(
    """
<style>
    /* Minimal, additive CSS only.
       The core theme lives in `.streamlit/config.toml`. */
    [data-testid="stMetric"] {
        background-color: rgba(248, 250, 252, 1);
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid rgba(226, 232, 240, 1);
    }
</style>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# SIDEBAR
# =============================================================================

with st.sidebar:
    st.title("📊 Data explorer")
    st.caption("Tagmarshal lakehouse pilot")

    st.divider()

    page = st.radio(
        "Navigate",
        [
            "Data quality",
            "Course topology",
            "Round insights",
            "Pace analysis",
            "Global metrics",
            "Infrastructure",
            "Data model",
            "Project context",
        ],
        label_visibility="collapsed",
    )

    st.divider()

    # Global course filter (used by pages that support it).
    # This replaces the old "Connected to database" text.
    course_filter_options = ["All courses"]
    if test_connection():
        try:
            courses_df = execute_query(
                "SELECT DISTINCT course_id FROM iceberg.silver.fact_telemetry_event ORDER BY course_id"
            )
            if courses_df is not None and not courses_df.empty:
                course_filter_options += courses_df["course_id"].astype(str).tolist()
        except Exception:
            # Fall back to "All courses" if we can't fetch course IDs.
            pass
    else:
        st.error(
            "Database unavailable. Check Trino on `http://localhost:8081` and your environment variables (TRINO_HOST/TRINO_PORT).",
            icon="❌",
        )

    # Callback to sync sidebar filter to topology filter
    def _sync_sidebar_to_topology():
        if "topology_course_filter" in st.session_state:
            st.session_state.topology_course_filter = st.session_state.course_filter

    st.selectbox(
        "Filter by course",
        options=course_filter_options,
        key="course_filter",
        help="Applies to pages that support course filtering.",
        on_change=_sync_sidebar_to_topology,
    )

    # Course legend (color + course type + loop indicator)
    if test_connection():
        try:
            # Use the enhanced query that joins with course profile
            course_summary_sidebar = execute_query(queries.COURSE_SUMMARY_WITH_PROFILE)
            if course_summary_sidebar is not None and not course_summary_sidebar.empty:
                course_summary_sidebar = course_summary_sidebar.copy()
                course_summary_sidebar["course_id"] = course_summary_sidebar[
                    "course_id"
                ].astype(str)
                course_ids_sidebar = course_summary_sidebar["course_id"].tolist()
                course_color_map_sidebar = get_course_color_map(course_ids_sidebar)

                st.caption("Course legend")
                legend_lines = []
                for _, row in course_summary_sidebar.sort_values(
                    "course_id"
                ).iterrows():
                    cid = str(row.get("course_id", ""))
                    # Use course_type from profile if available, otherwise inferred_type
                    course_type = str(
                        row.get("course_type", row.get("inferred_type", ""))
                    )
                    is_loop = row.get("is_loop_course", False)
                    loop_badge = " 🔄" if is_loop else ""
                    color = course_color_map_sidebar.get(cid, COLOR_SECONDARY)
                    legend_lines.append(
                        f"""
<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
  <span style="width:10px;height:10px;border-radius:999px;background:{color};display:inline-block;"></span>
  <span style="font-size:0.9rem;"><b>{cid}</b> — {course_type}{loop_badge}</span>
</div>
""".strip()
                    )
                st.markdown("\n".join(legend_lines), unsafe_allow_html=True)
        except Exception:
            # Sidebar legend is optional; ignore failures.
            pass


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _dedent_sql(sql: str) -> str:
    """Normalize SQL formatting for display in the UI."""
    return textwrap.dedent(sql).strip()


@dataclass(frozen=True)
class QueryResult:
    name: str
    sql: str
    df: Optional[pd.DataFrame]
    ok: bool
    elapsed_s: float
    error: Optional[BaseException] = None


def run_query(*, name: str, sql: str) -> QueryResult:
    """Execute a query and return a QueryResult (no UI rendering)."""
    start = time.perf_counter()
    try:
        df = execute_query(sql)
        return QueryResult(
            name=name,
            sql=sql,
            df=df,
            ok=True,
            elapsed_s=time.perf_counter() - start,
            error=None,
        )
    except Exception as e:
        return QueryResult(
            name=name,
            sql=sql,
            df=None,
            ok=False,
            elapsed_s=time.perf_counter() - start,
            error=e,
        )


def render_query_debug(q: QueryResult, *, show_debug: bool = True) -> None:
    """Render the (collapsed) SQL/debug expander at the end of a section."""
    if not show_debug:
        return

    if q.ok:
        with st.expander(f"🗄️ {q.name}", expanded=False):
            st.code(_dedent_sql(q.sql), language="sql")
            st.caption(f"⏱️ {q.name}: {q.elapsed_s:.2f}s")
    else:
        st.error(f"{q.name} failed: {q.error}")
        with st.expander(f"🗄️ {q.name}", expanded=True):
            st.code(_dedent_sql(q.sql), language="sql")
            if q.error is not None:
                st.code("".join(traceback.format_exception(q.error)), language="text")
            st.caption(f"⏱️ {q.name}: {q.elapsed_s:.2f}s")


def format_number(n):
    """Format large numbers with commas."""
    if n is None:
        return "—"
    return f"{n:,}"


def display_df(df, height=None):
    """Display a dataframe with consistent styling."""
    if df is None or df.empty:
        st.info("No data available")
        return

    # Apply consistent, readable number formatting across all tables.
    # We keep the table interactive by using Streamlit's `column_config`.
    column_config: dict = {}
    for col in df.columns:
        series = df[col]
        try:
            is_int = pd.api.types.is_integer_dtype(series)
            is_float = pd.api.types.is_float_dtype(series)
        except Exception:
            is_int, is_float = False, False

        if is_int:
            # Streamlit supports "localized" for comma/grouping formatting (per locale).
            column_config[col] = st.column_config.NumberColumn(format="localized")
        elif is_float:
            # Percent-like columns are easier to read with a single decimal.
            col_l = str(col).lower()
            if "pct" in col_l or "%" in col_l or "percent" in col_l:
                # printf-style format string (see sprintf.js in Streamlit docs)
                column_config[col] = st.column_config.NumberColumn(format="%.1f")
            else:
                # Use localized commas; keep float precision inferred by Streamlit.
                column_config[col] = st.column_config.NumberColumn(format="localized")

    # Only set height if explicitly provided (allows auto-sizing)
    dataframe_kwargs = {
        "use_container_width": True,
        "hide_index": True,
        "column_config": column_config,
    }
    if height is not None:
        dataframe_kwargs["height"] = height

    st.dataframe(df, **dataframe_kwargs)


# =============================================================================
# PAGE: DATA QUALITY
# =============================================================================


def _get_quality_color(score: float) -> str:
    """Get colour for quality score using centralized color palette."""
    if score >= 80:
        return COLOR_GOOD  # Green - good/success
    elif score >= 60:
        return COLOR_WARNING  # Amber/yellow - warning/moderate
    else:
        return COLOR_CRITICAL  # Red - critical/bad


def create_quality_donut(score: float, title: str) -> go.Figure:
    """Create a full circle donut chart for data quality score."""
    color = _get_quality_color(score)

    # Calculate remaining percentage
    remaining = 100 - score

    # Create donut chart with two segments: score (coloured) and remainder (light grey)
    fig = go.Figure(
        data=[
            go.Pie(
                values=[score, remaining],
                hole=0.6,  # Creates the donut effect (60% hole)
                marker_colors=[color, COLOR_BORDER],
                textinfo="none",  # Hide default text
                showlegend=False,
                rotation=90,  # Start from top
            )
        ]
    )

    # Add score text in the centre
    fig.add_annotation(
        text=f"<b>{score:.1f}%</b>",
        x=0.5,
        y=0.5,
        font_size=20,
        font_color=COLOR_TEXT,
        showarrow=False,
    )

    # Add course name below the donut
    fig.add_annotation(
        text=title,
        x=0.5,
        y=-0.15,
        font_size=14,
        font_color=COLOR_TEXT,
        showarrow=False,
    )

    fig.update_layout(
        height=220,
        margin=dict(l=10, r=10, t=10, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def render_data_quality():
    st.header("Data quality")
    st.markdown(
        "Current state of the pilot data in the lakehouse and understanding data completeness for building reliable analytics."
    )

    # High-level stats
    st.subheader("Data overview")
    overview_q = run_query(name="OVERVIEW_STATS", sql=queries.OVERVIEW_STATS)
    if overview_q.ok and overview_q.df is not None and not overview_q.df.empty:
        row = overview_q.df.iloc[0]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Courses", format_number(row.get("total_courses")))

        with col2:
            st.metric("Rounds", format_number(row.get("total_rounds")))

        with col3:
            st.metric("Total events", format_number(row.get("total_events")))

        with col4:
            st.metric("Real events", format_number(row.get("real_events")))

        # Date range
        st.markdown(
            f"**Date range:** {row.get('earliest_date')} to {row.get('latest_date')}"
        )

    render_query_debug(overview_q)
    st.divider()

    # Column completeness
    st.subheader("Column completeness detail")
    st.markdown(
        "Percentage of non-null values for key columns (excluding padding data)."
    )

    completeness_q = run_query(
        name="COLUMN_COMPLETENESS", sql=queries.COLUMN_COMPLETENESS
    )
    completeness = completeness_q.df
    if completeness_q.ok and completeness is not None and not completeness.empty:
        # Create heatmap of completeness percentages
        # Prepare data for heatmap: courses as rows, columns as columns
        # Use the same columns in the same order for both heatmap and table
        # Exclude course_id and total_events from the heatmap (they're not percentages)
        heatmap_cols = [
            "pace_pct",
            "pace_gap_pct",
            "hole_pct",
            "section_pct",
            "gps_pct",
            "fix_timestamp_pct",
            "start_hole_pct",
            "start_section_pct",
            "is_complete_pct",
            "battery_pct",
            "device_pct",
        ]
        # Shorter labels for x-axis (remove _pct suffix)
        heatmap_labels = [
            "pace",
            "pace_gap",
            "hole",
            "section",
            "gps",
            "fix_timestamp",
            "start_hole",
            "start_section",
            "is_complete",
            "battery",
            "device",
        ]

        # Build matrix for heatmap
        courses = completeness["course_id"].tolist()
        heatmap_data = []
        for _, row in completeness.iterrows():
            row_data = [float(row.get(col, 0)) for col in heatmap_cols]
            heatmap_data.append(row_data)

        # Create heatmap
        fig_heatmap = go.Figure(
            data=go.Heatmap(
                z=heatmap_data,
                x=heatmap_labels,
                y=courses,
                colorscale=COLOR_SCALE_QUALITY,
                text=[[f"{val:.1f}%" for val in row] for row in heatmap_data],
                texttemplate="%{text}",
                textfont={"size": 11},
                showscale=False,
                hovertemplate="<b>%{y}</b><br>%{x}: %{z:.1f}%<extra></extra>",
            )
        )

        fig_heatmap.update_layout(
            height=300,
            margin=dict(l=100, r=20, t=20, b=40),
            xaxis_title="",
            yaxis_title="",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )

        st.plotly_chart(fig_heatmap, use_container_width=True)

        st.markdown("---")  # Small divider before table

        # Keep column names as lowercase snake_case (same as heatmap)
        completeness_view = completeness.copy()
        display_df(completeness_view)

    render_query_debug(completeness_q)
    st.divider()

    # Data quality scores
    st.subheader("Data quality scores by course")
    st.markdown(
        "Composite quality score (0–100) based on column completeness, weighted by importance."
    )

    with st.expander("View scoring methodology", expanded=False):
        st.markdown(
            """
**Core Telemetry (40%)**
| Column | Weight |
|--------|--------|
| `pace` | 15% |
| `latitude` + `longitude` (GPS) | 10% |
| `pace_gap` | 5% |
| `positional_gap` | 5% |
| `fix_timestamp` | 5% |

**Position Tracking (25%)**
| Column | Weight |
|--------|--------|
| `hole_number` | 8% |
| `section_number` | 8% |
| `location_index` | 5% |
| `current_hole` | 2% |
| `current_hole_section` | 2% |

**Round Context (20%)**
| Column | Weight |
|--------|--------|
| `round_start_time` | 5% |
| `round_end_time` | 5% |
| `start_hole` | 4% |
| `start_section` | 3% |
| `is_complete` | 3% |

**Device Health (15%)**
| Column | Weight |
|--------|--------|
| `device` | 10% |
| `battery_percentage` | 5% |
            """
        )

    quality_q = run_query(name="DATA_QUALITY_SCORE", sql=queries.DATA_QUALITY_SCORE)
    quality_df = quality_q.df
    if quality_q.ok and quality_df is not None and not quality_df.empty:
        # Display all courses in a single row using donut charts
        num_courses = len(quality_df)
        cols = st.columns(num_courses)
        for i, (_, row) in enumerate(quality_df.iterrows()):
            with cols[i]:
                score = float(row["quality_score"]) if row["quality_score"] else 0
                fig = create_quality_donut(score, row["course_id"])
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Quality breakdown table
        st.markdown("**Quality score breakdown**")
        # Keep this resilient to query changes by selecting only columns that exist.
        display_cols = [
            "course_id",
            "total_events",
            "pace_pct",
            "gps_complete_pct",
            "fix_timestamp_pct",
            "hole_pct",
            "device_pct",
            "quality_score",
        ]
        missing = [c for c in display_cols if c not in quality_df.columns]
        if missing:
            st.warning(
                f"Quality breakdown: missing columns in result: {missing}. Showing available columns only."
            )
        available_cols = [c for c in display_cols if c in quality_df.columns]
        quality_breakdown_df = quality_df[available_cols].copy()

        rename_map = {
            "course_id": "Course",
            "total_events": "Events",
            "pace_pct": "Pace %",
            "gps_complete_pct": "GPS %",
            "fix_timestamp_pct": "Timestamp %",
            "hole_pct": "Hole %",
            "device_pct": "Device %",
            "quality_score": "Score",
        }
        quality_breakdown_df = quality_breakdown_df.rename(columns=rename_map)
        display_df(quality_breakdown_df)

    render_query_debug(quality_q)
    st.divider()

    # Padding analysis
    st.subheader("Padding data analysis")
    st.markdown(
        "'Padding' represents placeholder data where telemetry wasn't collected."
    )

    padding_q = run_query(name="PADDING_ANALYSIS", sql=queries.PADDING_ANALYSIS)
    padding_df = padding_q.df
    if padding_q.ok and padding_df is not None and not padding_df.empty:
        # Create stacked bar chart
        fig_padding = go.Figure()

        # Add real events bar (bottom stack)
        fig_padding.add_trace(
            go.Bar(
                name="real_events",
                x=padding_df["course_id"],
                y=padding_df["real_events"],
                marker_color=COLOR_GOOD,  # Green for real events
            )
        )

        # Add padding events bar (top stack)
        fig_padding.add_trace(
            go.Bar(
                name="padding_events",
                x=padding_df["course_id"],
                y=padding_df["padding_events"],
                marker_color=COLOR_CRITICAL,  # Red for padding events
            )
        )

        fig_padding.update_layout(
            barmode="stack",
            height=400,
            margin=dict(l=20, r=20, t=20, b=40),
            xaxis_title="Course",
            yaxis_title="Number of events",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )

        st.plotly_chart(fig_padding, use_container_width=True)

        # Detailed table in collapsible container (collapsed by default)
        with st.expander("📋 Detailed padding data analysis", expanded=False):
            display_df(padding_df)

    render_query_debug(padding_q)


# =============================================================================
# PAGE: ROUND INSIGHTS
# =============================================================================


def render_round_insights():
    st.header("Round insights")
    st.markdown(
        """
    Analyze round data quality, duration patterns, and progression sequences.
    Use the tabs below to explore different aspects of round data.
    """
    )

    selected_course = st.session_state.get("course_filter", "All courses")

    # Create tabs for different insights
    tab_overview, tab_duration, tab_validation, tab_progression = st.tabs(
        ["📊 Overview", "⏱️ Duration Analysis", "✅ Validation", "📈 Progression"]
    )

    # =========================================================================
    # TAB 1: OVERVIEW
    # =========================================================================
    with tab_overview:
        st.subheader("Round length distribution")
        st.markdown(
            "Rounds are grouped by the number of distinct holes visited (non-padding events)."
        )

        round_len_q = run_query(
            name="ROUND_LENGTH_DISTRIBUTION", sql=queries.ROUND_LENGTH_DISTRIBUTION
        )
        try:
            round_len = round_len_q.df
            if round_len_q.ok and round_len is not None and not round_len.empty:
                round_len = round_len.copy()
                round_len["course_id"] = round_len["course_id"].astype(str)

                bucket_order = ["<9", "9", "18", "27", ">27", "other (10–26)"]
                round_len["round_length_bucket"] = pd.Categorical(
                    round_len["round_length_bucket"],
                    categories=bucket_order,
                    ordered=True,
                )

                if selected_course == "All courses":
                    fig = px.bar(
                        round_len,
                        x="course_id",
                        y="round_count",
                        color="round_length_bucket",
                        barmode="stack",
                        category_orders={"round_length_bucket": bucket_order},
                        color_discrete_sequence=COLOR_SEQUENCE,
                    )
                    fig.update_layout(
                        height=320,
                        margin=dict(l=20, r=20, t=40, b=40),
                        xaxis_title="course_id",
                        yaxis_title="round_count",
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1,
                        ),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        title="Round length buckets by course",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    course_df = round_len[
                        round_len["course_id"] == str(selected_course)
                    ].copy()
                    fig = px.bar(
                        course_df,
                        x="round_length_bucket",
                        y="round_count",
                        color="round_length_bucket",
                        category_orders={"round_length_bucket": bucket_order},
                        color_discrete_sequence=COLOR_SEQUENCE,
                    )
                    fig.update_layout(
                        height=280,
                        margin=dict(l=20, r=20, t=40, b=40),
                        xaxis_title="round_length_bucket",
                        yaxis_title="round_count",
                        showlegend=False,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        title=f"Round length buckets for {selected_course}",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                pivot = (
                    round_len.pivot_table(
                        index="course_id",
                        columns="round_length_bucket",
                        values="round_count",
                        aggfunc="sum",
                        fill_value=0,
                    )
                    .reset_index()
                    .copy()
                )
                pivot["total_rounds"] = pivot[
                    [c for c in pivot.columns if c != "course_id"]
                ].sum(axis=1)

                if selected_course != "All courses":
                    pivot = pivot[pivot["course_id"] == str(selected_course)]

                display_df(pivot)
                render_query_debug(round_len_q)
            else:
                st.info("No round-length data available.")
                render_query_debug(round_len_q)
        except Exception as e:
            st.error(f"Could not load round length distribution: {e}")
            render_query_debug(round_len_q)

        st.divider()

        # Which nines were played?
        st.subheader("Nines played")
        st.markdown(
            "For each round, we identify the set of `nine_number` values observed (e.g., `1+2`), which helps distinguish 27-hole combinations."
        )

        if selected_course == "All courses":
            nines_q = run_query(
                name="ROUND_NINE_COMBINATIONS", sql=queries.ROUND_NINE_COMBINATIONS
            )
        else:
            nines_q = run_query(
                name="get_round_nine_combinations_for_course",
                sql=queries.get_round_nine_combinations_for_course(
                    str(selected_course)
                ),
            )

        try:
            nines_df = nines_q.df
            if nines_q.ok and nines_df is not None and not nines_df.empty:
                nines_df = nines_df.copy()
                if "course_id" in nines_df.columns:
                    nines_df["course_id"] = nines_df["course_id"].astype(str)

                bucket_order = ["<9", "9", "18", "27", ">27", "other (10–26)"]

                if selected_course == "All courses":
                    agg = (
                        nines_df.groupby(["holes_played_bucket"], as_index=False)[
                            "round_count"
                        ]
                        .sum()
                        .copy()
                    )
                    agg["holes_played_bucket"] = pd.Categorical(
                        agg["holes_played_bucket"],
                        categories=bucket_order,
                        ordered=True,
                    )
                    agg = agg.sort_values("holes_played_bucket")
                    fig = px.bar(
                        agg,
                        x="holes_played_bucket",
                        y="round_count",
                        color="holes_played_bucket",
                        category_orders={"holes_played_bucket": bucket_order},
                        color_discrete_sequence=COLOR_SEQUENCE,
                    )
                    fig.update_layout(
                        height=260,
                        margin=dict(l=20, r=20, t=40, b=40),
                        xaxis_title="holes_played_bucket",
                        yaxis_title="round_count",
                        showlegend=False,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        title="Overall holes-played mix (all courses)",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    with st.expander("📋 View nines played table", expanded=False):
                        display_df(
                            nines_df.sort_values(
                                ["course_id", "holes_played_bucket", "round_count"],
                                ascending=[True, True, False],
                            )
                        )
                else:
                    nines_df["holes_played_bucket"] = pd.Categorical(
                        nines_df["holes_played_bucket"],
                        categories=bucket_order,
                        ordered=True,
                    )
                    fig = px.bar(
                        nines_df.sort_values(
                            ["holes_played_bucket", "round_count"],
                            ascending=[True, False],
                        ),
                        x="nines_played",
                        y="round_count",
                        color="holes_played_bucket",
                        barmode="stack",
                        category_orders={"holes_played_bucket": bucket_order},
                        color_discrete_sequence=COLOR_SEQUENCE,
                    )
                    fig.update_layout(
                        height=300,
                        margin=dict(l=20, r=20, t=40, b=40),
                        xaxis_title="nines_played",
                        yaxis_title="round_count",
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1,
                        ),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        title=f"Nines played for {selected_course}",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    display_df(nines_df)

                render_query_debug(nines_q)
            else:
                st.info("No nines-played data available.")
                render_query_debug(nines_q)
        except Exception as e:
            st.error(f"Could not load nines played: {e}")
            render_query_debug(nines_q)

    # =========================================================================
    # TAB 2: DURATION ANALYSIS
    # =========================================================================
    with tab_duration:
        st.subheader("Round duration analysis")
        st.markdown(
            """
            Analyze round and hole durations to identify outliers and patterns.
            Outliers are highlighted based on statistical thresholds (IQR method).
            """
        )

        # Get course list for selection
        courses_df_q = run_query(name="COURSE_SUMMARY", sql=queries.COURSE_SUMMARY)
        courses_df = courses_df_q.df

        if courses_df_q.ok and courses_df is not None and not courses_df.empty:
            course_options = courses_df["course_id"].astype(str).tolist()
            default_course = (
                str(selected_course)
                if selected_course != "All courses"
                and str(selected_course) in course_options
                else course_options[0]
            )

            col1, col2 = st.columns([1, 3])
            with col1:
                duration_course = st.selectbox(
                    "Select course",
                    options=course_options,
                    index=course_options.index(default_course),
                    key="duration_analysis_course",
                )

            # Fetch round duration data
            duration_q = run_query(
                name="ROUND_DURATION_DETAILS",
                sql=queries.get_round_duration_for_course(duration_course),
            )

            if duration_q.ok and duration_q.df is not None and not duration_q.df.empty:
                duration_df = duration_q.df.copy()

                # Calculate outlier thresholds using IQR
                q1 = duration_df["duration_minutes"].quantile(0.25)
                q3 = duration_df["duration_minutes"].quantile(0.75)
                iqr = q3 - q1
                lower_bound = max(0, q1 - 1.5 * iqr)
                upper_bound = q3 + 1.5 * iqr

                # Mark outliers
                duration_df["is_outlier"] = (
                    duration_df["duration_minutes"] < lower_bound
                ) | (duration_df["duration_minutes"] > upper_bound)
                duration_df["outlier_type"] = duration_df.apply(
                    lambda row: (
                        "too_short"
                        if row["duration_minutes"] < lower_bound
                        else (
                            "too_long"
                            if row["duration_minutes"] > upper_bound
                            else "normal"
                        )
                    ),
                    axis=1,
                )

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total rounds", len(duration_df))
                with col2:
                    st.metric(
                        "Avg duration",
                        f"{duration_df['duration_minutes'].mean():.0f} min",
                    )
                with col3:
                    st.metric(
                        "Median duration",
                        f"{duration_df['duration_minutes'].median():.0f} min",
                    )
                with col4:
                    outlier_count = duration_df["is_outlier"].sum()
                    st.metric(
                        "Outliers",
                        f"{outlier_count} ({100*outlier_count/len(duration_df):.1f}%)",
                    )

                st.markdown("")

                # Duration distribution histogram with outliers highlighted
                fig_hist = px.histogram(
                    duration_df,
                    x="duration_minutes",
                    color="outlier_type",
                    nbins=30,
                    color_discrete_map={
                        "normal": COLOR_PRIMARY,
                        "too_short": "#e74c3c",
                        "too_long": "#e67e22",
                    },
                    category_orders={
                        "outlier_type": ["normal", "too_short", "too_long"]
                    },
                )
                fig_hist.add_vline(
                    x=lower_bound,
                    line_dash="dash",
                    line_color="#e74c3c",
                    annotation_text=f"Lower: {lower_bound:.0f}",
                )
                fig_hist.add_vline(
                    x=upper_bound,
                    line_dash="dash",
                    line_color="#e67e22",
                    annotation_text=f"Upper: {upper_bound:.0f}",
                )
                fig_hist.update_layout(
                    height=300,
                    margin=dict(l=20, r=20, t=40, b=40),
                    xaxis_title="Duration (minutes)",
                    yaxis_title="Count",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    title="Round duration distribution with outliers",
                    legend=dict(title="Status"),
                )
                st.plotly_chart(fig_hist, use_container_width=True)

                # Box plot by round type
                fig_box = px.box(
                    duration_df,
                    x="round_type",
                    y="duration_minutes",
                    color="round_type",
                    points="outliers",
                    color_discrete_sequence=[COLOR_PRIMARY, COLOR_SECONDARY],
                )
                fig_box.update_layout(
                    height=300,
                    margin=dict(l=20, r=20, t=40, b=40),
                    xaxis_title="Round type",
                    yaxis_title="Duration (minutes)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    title="Duration by round type",
                    showlegend=False,
                )
                st.plotly_chart(fig_box, use_container_width=True)

                # Outliers table
                with st.expander(
                    f"📋 View outlier rounds ({outlier_count} rounds)", expanded=False
                ):
                    outliers_df = duration_df[duration_df["is_outlier"]].copy()
                    if not outliers_df.empty:
                        display_df(
                            outliers_df[
                                [
                                    "round_id",
                                    "round_date",
                                    "duration_minutes",
                                    "round_type",
                                    "holes_visited",
                                    "outlier_type",
                                ]
                            ].sort_values("duration_minutes")
                        )
                    else:
                        st.info("No outliers detected.")

                render_query_debug(duration_q)

                st.divider()

                # Per-hole duration analysis
                st.subheader("Per-hole duration")
                st.markdown(
                    "Average time spent on each hole. Helps identify slow or problematic holes."
                )

                hole_duration_q = run_query(
                    name="HOLE_DURATION",
                    sql=queries.get_hole_duration_for_course(duration_course),
                )

                if (
                    hole_duration_q.ok
                    and hole_duration_q.df is not None
                    and not hole_duration_q.df.empty
                ):
                    hole_df = hole_duration_q.df.copy()

                    # Bar chart of average hole duration
                    fig_hole = px.bar(
                        hole_df,
                        x="hole_number",
                        y="avg_duration_min",
                        error_y="stddev_duration",
                        color_discrete_sequence=[COLOR_PRIMARY],
                    )
                    fig_hole.update_layout(
                        height=300,
                        margin=dict(l=20, r=20, t=40, b=40),
                        xaxis_title="Hole number",
                        yaxis_title="Avg duration (minutes)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        title="Average duration per hole",
                    )
                    st.plotly_chart(fig_hole, use_container_width=True)

                    with st.expander("📋 View hole duration table", expanded=False):
                        display_df(hole_df)

                    render_query_debug(hole_duration_q)
                else:
                    st.info("No per-hole duration data available.")

            else:
                st.info("No duration data available for this course.")
                render_query_debug(duration_q)
        else:
            st.error("Could not load course list.")

    # =========================================================================
    # TAB 3: VALIDATION
    # =========================================================================
    with tab_validation:
        st.subheader("Round validation")

        # Explanation section
        with st.expander("ℹ️ How validation works", expanded=True):
            st.markdown(
                """
                ### Validation process

                Each round is validated against multiple criteria to ensure data quality:

                | Check | Description | Thresholds |
                |-------|-------------|------------|
                | **Duration** | Round duration should be realistic | 9-hole: 45-200 min, 18-hole: 90-400 min |
                | **Events** | Minimum telemetry events required | 9-hole: ≥9 events, 18-hole: ≥18 events |
                | **Pace data** | Sufficient pace calculations | ≥50% of events should have pace data |
                | **Sequence** | Holes should follow logical order | Start hole matches min hole observed |

                Rounds failing validation may indicate:
                - Incomplete data transmission
                - Device issues during the round
                - Unusual playing patterns (e.g., practice, partial rounds)
                """
            )

        st.divider()

        # Interactive threshold controls
        st.markdown("### Adjust validation thresholds")
        st.caption(
            "Modify thresholds to see how they affect validation results. Changes apply to the visualization below."
        )

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**9-hole rounds**")
            nine_min_duration = st.slider(
                "Min duration (min)",
                min_value=0,
                max_value=120,
                value=45,
                key="nine_min_dur",
            )
            nine_max_duration = st.slider(
                "Max duration (min)",
                min_value=60,
                max_value=300,
                value=200,
                key="nine_max_dur",
            )
            nine_min_events = st.slider(
                "Min events", min_value=1, max_value=50, value=9, key="nine_min_events"
            )

        with col2:
            st.markdown("**18-hole rounds**")
            eighteen_min_duration = st.slider(
                "Min duration (min)",
                min_value=0,
                max_value=180,
                value=90,
                key="eighteen_min_dur",
            )
            eighteen_max_duration = st.slider(
                "Max duration (min)",
                min_value=120,
                max_value=600,
                value=400,
                key="eighteen_max_dur",
            )
            eighteen_min_events = st.slider(
                "Min events",
                min_value=1,
                max_value=100,
                value=18,
                key="eighteen_min_events",
            )

        pace_threshold = st.slider(
            "Min pace data coverage (%)",
            min_value=0,
            max_value=100,
            value=50,
            key="pace_threshold",
        )

        st.divider()

        # Validation summary
        st.markdown("### Validation summary")

        validation_summary_q = run_query(
            name="ROUND_VALIDATION_SUMMARY", sql=queries.ROUND_VALIDATION_SUMMARY
        )
        validation_summary = validation_summary_q.df
        if (
            validation_summary_q.ok
            and validation_summary is not None
            and not validation_summary.empty
        ):
            summary_df = validation_summary[
                [
                    "course_id",
                    "total_rounds",
                    "pct_duration_valid",
                    "pct_events_valid",
                    "pct_pace_valid",
                ]
            ].copy()
            summary_df["course_id"] = summary_df["course_id"].astype(str)
            if selected_course != "All courses":
                summary_df = summary_df[summary_df["course_id"] == str(selected_course)]

            # Create a heatmap-style visualization
            fig_validation = go.Figure()

            for i, row in summary_df.iterrows():
                course = row["course_id"]
                metrics = ["Duration", "Events", "Pace"]
                values = [
                    row["pct_duration_valid"],
                    row["pct_events_valid"],
                    row["pct_pace_valid"],
                ]

                for j, (metric, value) in enumerate(zip(metrics, values)):
                    color = (
                        "#27ae60"
                        if value >= 80
                        else "#f39c12" if value >= 50 else "#e74c3c"
                    )
                    fig_validation.add_trace(
                        go.Bar(
                            x=[course],
                            y=[value],
                            name=metric,
                            marker_color=color,
                            text=[f"{value:.1f}%"],
                            textposition="auto",
                            showlegend=i == 0,
                            legendgroup=metric,
                        )
                    )

            fig_validation.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=40, b=40),
                barmode="group",
                xaxis_title="Course",
                yaxis_title="Validation pass rate (%)",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                title="Validation pass rates by course",
            )
            st.plotly_chart(fig_validation, use_container_width=True)

            display_df(summary_df)
        render_query_debug(validation_summary_q)

        # Detailed validation per round
        with st.expander("📋 Validation details per round", expanded=False):
            courses_df_q = run_query(name="COURSE_SUMMARY", sql=queries.COURSE_SUMMARY)
            courses_df = courses_df_q.df
            if courses_df_q.ok and courses_df is not None and not courses_df.empty:
                options = courses_df["course_id"].astype(str).tolist()
                default = (
                    str(selected_course)
                    if selected_course != "All courses"
                    and str(selected_course) in options
                    else options[0]
                )
                idx = options.index(default)
                chosen_course = st.selectbox(
                    "Course",
                    options=options,
                    index=idx,
                    key="round_insights_validation_course",
                )
                course_validation_q = run_query(
                    name="ROUND_VALIDATION",
                    sql=queries.get_round_validation_for_course(chosen_course),
                )
                display_df(course_validation_q.df)
                render_query_debug(course_validation_q)
            render_query_debug(courses_df_q)

    # =========================================================================
    # TAB 4: PROGRESSION ANALYSIS
    # =========================================================================
    with tab_progression:
        st.subheader("Round progression analysis")
        st.markdown(
            """
            Analyze whether rounds progress in a logical, sequential manner.
            This helps identify data quality issues regardless of where on the course a round starts
            (hole 1, shotgun start, or any nine on a 27-hole course).
            """
        )

        # Get course list
        courses_df_q = run_query(name="COURSE_SUMMARY", sql=queries.COURSE_SUMMARY)
        courses_df = courses_df_q.df

        if courses_df_q.ok and courses_df is not None and not courses_df.empty:
            course_options = courses_df["course_id"].astype(str).tolist()
            default_course = (
                str(selected_course)
                if selected_course != "All courses"
                and str(selected_course) in course_options
                else course_options[0]
            )

            col1, col2 = st.columns([1, 3])
            with col1:
                progression_course = st.selectbox(
                    "Select course",
                    options=course_options,
                    index=course_options.index(default_course),
                    key="progression_analysis_course",
                )

            # Progression summary for all rounds
            progression_summary_q = run_query(
                name="ROUND_PROGRESSION_SUMMARY",
                sql=queries.get_round_progression_summary(progression_course),
            )

            if (
                progression_summary_q.ok
                and progression_summary_q.df is not None
                and not progression_summary_q.df.empty
            ):
                prog_df = progression_summary_q.df.copy()

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total rounds", len(prog_df))
                with col2:
                    clean_count = (prog_df["progression_quality"] == "clean").sum()
                    st.metric(
                        "Clean progression",
                        f"{clean_count} ({100*clean_count/len(prog_df):.1f}%)",
                    )
                with col3:
                    minor_count = (
                        prog_df["progression_quality"] == "minor_issues"
                    ).sum()
                    st.metric(
                        "Minor issues",
                        f"{minor_count} ({100*minor_count/len(prog_df):.1f}%)",
                    )
                with col4:
                    review_count = (
                        prog_df["progression_quality"] == "needs_review"
                    ).sum()
                    st.metric(
                        "Needs review",
                        f"{review_count} ({100*review_count/len(prog_df):.1f}%)",
                    )

                st.markdown("")

                # Progression quality distribution
                quality_counts = prog_df["progression_quality"].value_counts()
                fig_quality = px.pie(
                    values=quality_counts.values,
                    names=quality_counts.index,
                    color=quality_counts.index,
                    color_discrete_map={
                        "clean": "#27ae60",
                        "minor_issues": "#f39c12",
                        "needs_review": "#e74c3c",
                    },
                )
                fig_quality.update_layout(
                    height=300,
                    margin=dict(l=20, r=20, t=40, b=40),
                    title="Progression quality distribution",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_quality, use_container_width=True)

                # Anomaly scatter plot
                fig_scatter = px.scatter(
                    prog_df,
                    x="hole_anomalies",
                    y="section_anomalies",
                    color="progression_quality",
                    size="total_events",
                    hover_data=["round_id", "start_hole", "holes_visited"],
                    color_discrete_map={
                        "clean": "#27ae60",
                        "minor_issues": "#f39c12",
                        "needs_review": "#e74c3c",
                    },
                )
                fig_scatter.update_layout(
                    height=350,
                    margin=dict(l=20, r=20, t=40, b=40),
                    xaxis_title="Hole anomalies",
                    yaxis_title="Section anomalies",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    title="Anomaly distribution by round",
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

                # Rounds needing review
                with st.expander(
                    f"📋 Rounds needing review ({review_count} rounds)", expanded=False
                ):
                    review_df = prog_df[
                        prog_df["progression_quality"] == "needs_review"
                    ].copy()
                    if not review_df.empty:
                        display_df(
                            review_df[
                                [
                                    "round_id",
                                    "total_events",
                                    "start_hole",
                                    "end_hole",
                                    "holes_visited",
                                    "hole_anomalies",
                                    "section_anomalies",
                                ]
                            ].sort_values("hole_anomalies", ascending=False)
                        )
                    else:
                        st.success("No rounds need review!")

                render_query_debug(progression_summary_q)

                st.divider()

                # Detailed round progression viewer
                st.subheader("Detailed round progression")
                st.markdown(
                    "Select a round to see its event-by-event progression and identify specific issues."
                )

                rounds_q = run_query(name="ROUND_LIST", sql=queries.ROUND_LIST)
                rounds = rounds_q.df
                if rounds_q.ok and rounds is not None and not rounds.empty:
                    rounds = rounds.copy()
                    rounds["course_id"] = rounds["course_id"].astype(str)
                    course_rounds = rounds[
                        rounds["course_id"] == progression_course
                    ].copy()

                    if not course_rounds.empty:
                        round_options = [
                            f"{r['round_id']} ({r['round_date']}, {r['event_count']} events)"
                            for _, r in course_rounds.iterrows()
                        ]
                        selected_round_idx = st.selectbox(
                            "Select round",
                            range(len(round_options)),
                            format_func=lambda i: round_options[i],
                            key="progression_round_select",
                        )
                        selected_round = course_rounds.iloc[selected_round_idx][
                            "round_id"
                        ]

                        # Get detailed progression
                        detail_q = run_query(
                            name="ROUND_PROGRESSION_DETAIL",
                            sql=queries.get_round_progression(
                                progression_course, selected_round
                            ),
                        )

                        if (
                            detail_q.ok
                            and detail_q.df is not None
                            and not detail_q.df.empty
                        ):
                            detail_df = detail_q.df.copy()

                            # Progression line chart
                            fig_prog = go.Figure()
                            fig_prog.add_trace(
                                go.Scatter(
                                    x=detail_df["event_sequence"],
                                    y=detail_df["hole_number"],
                                    mode="lines+markers",
                                    name="Hole",
                                    line=dict(color=COLOR_PRIMARY),
                                )
                            )
                            fig_prog.add_trace(
                                go.Scatter(
                                    x=detail_df["event_sequence"],
                                    y=detail_df["section_number"],
                                    mode="lines+markers",
                                    name="Section",
                                    line=dict(color=COLOR_SECONDARY),
                                    yaxis="y2",
                                )
                            )
                            fig_prog.update_layout(
                                height=350,
                                margin=dict(l=20, r=60, t=40, b=40),
                                xaxis_title="Event sequence",
                                yaxis_title="Hole number",
                                yaxis2=dict(
                                    title="Section number",
                                    overlaying="y",
                                    side="right",
                                ),
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="rgba(0,0,0,0)",
                                title=f"Progression for round {selected_round}",
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.02,
                                    xanchor="right",
                                    x=1,
                                ),
                            )
                            st.plotly_chart(fig_prog, use_container_width=True)

                            # Map visualization of round path
                            st.markdown("**Round path on map:**")
                            map_points_q = run_query(
                                name="ROUND_MAP_POINTS",
                                sql=queries.get_round_map_points(
                                    progression_course, selected_round
                                ),
                            )

                            if (
                                map_points_q.ok
                                and map_points_q.df is not None
                                and not map_points_q.df.empty
                            ):
                                map_df = map_points_q.df.copy()

                                # Create map with path line and markers
                                fig_map = go.Figure()

                                # Add the path as a line
                                fig_map.add_trace(
                                    go.Scattermapbox(
                                        lat=map_df["latitude"],
                                        lon=map_df["longitude"],
                                        mode="lines",
                                        line=dict(width=2, color=COLOR_PRIMARY),
                                        name="Path",
                                        hoverinfo="skip",
                                    )
                                )

                                # Add markers colored by hole number
                                fig_map.add_trace(
                                    go.Scattermapbox(
                                        lat=map_df["latitude"],
                                        lon=map_df["longitude"],
                                        mode="markers",
                                        marker=dict(
                                            size=8,
                                            color=map_df["hole_number"],
                                            colorscale="Viridis",
                                            showscale=True,
                                            colorbar=dict(title="Hole"),
                                        ),
                                        text=map_df.apply(
                                            lambda r: f"Event {int(r['event_sequence'])}<br>Hole {int(r['hole_number']) if pd.notna(r['hole_number']) else 'N/A'}<br>Section {int(r['section_number']) if pd.notna(r['section_number']) else 'N/A'}",
                                            axis=1,
                                        ),
                                        hoverinfo="text",
                                        name="Events",
                                    )
                                )

                                # Add start marker
                                fig_map.add_trace(
                                    go.Scattermapbox(
                                        lat=[map_df.iloc[0]["latitude"]],
                                        lon=[map_df.iloc[0]["longitude"]],
                                        mode="markers",
                                        marker=dict(
                                            size=14, color="#27ae60", symbol="circle"
                                        ),
                                        name="Start",
                                        hoverinfo="name",
                                    )
                                )

                                # Add end marker
                                fig_map.add_trace(
                                    go.Scattermapbox(
                                        lat=[map_df.iloc[-1]["latitude"]],
                                        lon=[map_df.iloc[-1]["longitude"]],
                                        mode="markers",
                                        marker=dict(
                                            size=14, color="#e74c3c", symbol="circle"
                                        ),
                                        name="End",
                                        hoverinfo="name",
                                    )
                                )

                                # Calculate center and zoom
                                center_lat = map_df["latitude"].mean()
                                center_lon = map_df["longitude"].mean()
                                lat_range = (
                                    map_df["latitude"].max() - map_df["latitude"].min()
                                )
                                lon_range = (
                                    map_df["longitude"].max()
                                    - map_df["longitude"].min()
                                )
                                max_range = max(lat_range, lon_range)

                                if max_range < 0.005:
                                    zoom = 17
                                elif max_range < 0.01:
                                    zoom = 16
                                elif max_range < 0.02:
                                    zoom = 15
                                elif max_range < 0.05:
                                    zoom = 14
                                else:
                                    zoom = 13

                                fig_map.update_layout(
                                    height=450,
                                    margin=dict(l=0, r=0, t=0, b=0),
                                    mapbox=dict(
                                        style="open-street-map",
                                        center=dict(lat=center_lat, lon=center_lon),
                                        zoom=zoom,
                                    ),
                                    showlegend=True,
                                    legend=dict(
                                        orientation="h",
                                        yanchor="top",
                                        y=0.99,
                                        xanchor="left",
                                        x=0.01,
                                        bgcolor="rgba(255,255,255,0.8)",
                                    ),
                                )

                                PLOTLY_MAP_CONFIG = {
                                    "scrollZoom": True,
                                    "displayModeBar": True,
                                    "displaylogo": False,
                                    "responsive": True,
                                }
                                st.plotly_chart(
                                    fig_map,
                                    use_container_width=True,
                                    config=PLOTLY_MAP_CONFIG,
                                    key="round_progression_map",
                                )
                                render_query_debug(map_points_q)
                            else:
                                st.info(
                                    "No GPS data available for this round to display on map."
                                )

                            st.markdown("")

                            # Transition analysis
                            transitions = detail_df["hole_transition"].value_counts()
                            st.markdown("**Hole transitions:**")
                            cols = st.columns(len(transitions))
                            for i, (trans, count) in enumerate(transitions.items()):
                                with cols[i]:
                                    color = (
                                        "normal"
                                        if trans in ["start", "same_hole", "next_hole"]
                                        else "inverse"
                                    )
                                    st.metric(trans, count)

                            with st.expander("📋 View detailed events", expanded=False):
                                display_df(detail_df)

                            render_query_debug(detail_q)
                    else:
                        st.info("No rounds available for this course.")
                render_query_debug(rounds_q)
            else:
                st.info("No progression data available for this course.")
                render_query_debug(progression_summary_q)
        else:
            st.error("Could not load course list.")


# =============================================================================
# PAGE: COURSE TOPOLOGY
# =============================================================================


def render_topology():
    st.header("Course topology")
    st.markdown(
        """
    Topology describes how each course is structured: which sections belong to which nine,
    and how many nines the course has.
    """
    )

    selected_course = st.session_state.get("course_filter", "All courses")

    # Load core datasets up front (topology + course summary)
    topology_q = run_query(name="TOPOLOGY", sql=queries.TOPOLOGY)
    course_q = run_query(name="COURSE_SUMMARY", sql=queries.COURSE_SUMMARY)
    try:
        topology = topology_q.df
        course_summary = course_q.df

        if not topology_q.ok or topology is None or topology.empty:
            st.info("No topology data available.")
            render_query_debug(topology_q)
            render_query_debug(course_q)
            return

        # Normalise types for safety
        topology = topology.copy()
        topology["course_id"] = topology["course_id"].astype(str)

        course_ids = set(topology["course_id"].unique().tolist())
        if course_q.ok and course_summary is not None and not course_summary.empty:
            try:
                course_ids |= set(
                    course_summary["course_id"].astype(str).unique().tolist()
                )
            except Exception:
                pass
        course_ids = sorted(course_ids)
        course_color_map = get_course_color_map(course_ids)
        if selected_course != "All courses" and selected_course not in course_ids:
            st.warning(
                f"Course filter '{selected_course}' is not present in topology results. Showing all courses."
            )
            selected_course = "All courses"

        st.divider()

        # ---------------------------------------------------------------------
        # 1) Course summary (top)
        # ---------------------------------------------------------------------
        st.subheader("Course summary")
        st.markdown(
            "Each course has been analysed to determine its type and data volume."
        )

        course_summary_view = course_summary
        if (
            course_q.ok
            and course_summary_view is not None
            and not course_summary_view.empty
        ):
            course_summary_view = course_summary_view.copy()
            course_summary_view["course_id"] = course_summary_view["course_id"].astype(
                str
            )
            # Always show all courses in the summary table (no filtering)
        display_df(course_summary_view)
        render_query_debug(course_q)

        st.divider()

        # ---------------------------------------------------------------------
        # 2) Nine boundaries (bar + section ranges)
        # ---------------------------------------------------------------------
        st.subheader("Nine boundaries")
        st.markdown(
            "We infer nine boundaries by grouping contiguous section ranges into course units."
        )

        # Bar chart: number of nines per course (always show all courses)
        nines_df = (
            topology.groupby("course_id", as_index=False)["nine_number"]
            .nunique()
            .rename(columns={"nine_number": "nines"})
            .sort_values("course_id")
        )

        fig_nines = px.bar(
            nines_df,
            x="course_id",
            y="nines",
            color="course_id",
            color_discrete_map=course_color_map,
        )
        fig_nines.update_layout(
            height=260,
            margin=dict(l=20, r=20, t=40, b=40),
            xaxis_title="course_id",
            yaxis_title="nines",
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Number of nines by course",
        )
        st.plotly_chart(fig_nines, use_container_width=True)

        render_query_debug(topology_q)

        st.divider()

        # ---------------------------------------------------------------------
        # 3) Course map
        # ---------------------------------------------------------------------

        # Plotly config: enable trackpad/wheel zoom + keep normal map interactions.
        PLOTLY_MAP_CONFIG = {
            "scrollZoom": True,  # critical for trackpad pinch / wheel zoom
            "displayModeBar": True,
            "displaylogo": False,
            "responsive": True,
        }

        def _plotly_map(fig: go.Figure, *, key: str) -> None:
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=key,
                config=PLOTLY_MAP_CONFIG,
            )

        def _map_center_zoom(df: pd.DataFrame) -> tuple[dict, int]:
            """Compute a good initial map centre/zoom that fits all points, without locking interactions."""
            lat_min = float(df["latitude"].min())
            lat_max = float(df["latitude"].max())
            lon_min = float(df["longitude"].min())
            lon_max = float(df["longitude"].max())

            # Pad a little so points at the edge aren't clipped.
            lat_span = max(lat_max - lat_min, 1e-6)
            lon_span = max(lon_max - lon_min, 1e-6)
            lat_pad = max(lat_span * 0.05, 0.001)
            lon_pad = max(lon_span * 0.05, 0.001)
            lat_span += 2 * lat_pad
            lon_span += 2 * lon_pad

            span = max(lat_span, lon_span)

            # Heuristic: smaller span => higher zoom (increased by 1-2 levels for tighter fit).
            if span > 60:
                zoom = 2
            elif span > 30:
                zoom = 3
            elif span > 15:
                zoom = 4
            elif span > 8:
                zoom = 5
            elif span > 4:
                zoom = 6
            elif span > 2:
                zoom = 7
            elif span > 1:
                zoom = 8
            elif span > 0.5:
                zoom = 9
            elif span > 0.25:
                zoom = 10
            elif span > 0.12:
                zoom = 11
            elif span > 0.06:
                zoom = 12
            elif span > 0.03:
                zoom = 13
            elif span > 0.015:
                zoom = 14
            elif span > 0.007:
                zoom = 15
            elif span > 0.0035:
                zoom = 16
            else:
                zoom = 17

            center = {
                "lat": float((lat_min + lat_max) / 2.0),
                "lon": float((lon_min + lon_max) / 2.0),
            }
            return center, zoom

        # Course filter - placed between heading and map
        topology_filter_options = ["All courses"] + course_ids
        current_idx = 0
        if selected_course in topology_filter_options:
            current_idx = topology_filter_options.index(selected_course)

        st.subheader("Course map")
        st.markdown(
            "Explore course locations and drill down into individual course topology."
        )

        # Callback to sync topology filter to sidebar filter
        def _sync_topology_to_sidebar():
            st.session_state.course_filter = st.session_state.topology_course_filter

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            selected_course = st.selectbox(
                "Filter by course",
                options=topology_filter_options,
                index=current_idx,
                key="topology_course_filter",
                help="Select a course to view its detailed topology.",
                on_change=_sync_topology_to_sidebar,
            )

        if selected_course == "All courses":
            st.caption(
                "One point per course, based on average GPS coordinates. Circle size represents the number of rounds played."
            )

            centroids_q = run_query(
                name="COURSE_CENTROIDS", sql=queries.COURSE_CENTROIDS
            )
            centroids = centroids_q.df
            if centroids_q.ok and centroids is not None and not centroids.empty:
                centroids = centroids.copy()
                centroids["course_id"] = centroids["course_id"].astype(str)

                # Join with course_summary to get round_count
                if (
                    course_q.ok
                    and course_summary is not None
                    and not course_summary.empty
                ):
                    course_summary_copy = course_summary.copy()
                    course_summary_copy["course_id"] = course_summary_copy[
                        "course_id"
                    ].astype(str)
                    centroids = centroids.merge(
                        course_summary_copy[["course_id", "round_count"]],
                        on="course_id",
                        how="left",
                    )
                    # Fill any missing round_count with 0
                    centroids["round_count"] = (
                        centroids["round_count"].fillna(0).astype(int)
                    )
                else:
                    # Fallback if course_summary is not available
                    centroids["round_count"] = 0

                center, zoom = _map_center_zoom(centroids)

                fig_map = px.scatter_mapbox(
                    centroids,
                    lat="latitude",
                    lon="longitude",
                    color="course_id",
                    size="round_count",
                    hover_name="course_id",
                    hover_data={
                        "round_count": True,
                        "event_count": True,
                        "projected_events": True,
                    },
                    custom_data=["course_id"],
                    color_discrete_map=course_color_map,
                    zoom=zoom,
                    center=center,
                    height=450,
                )
                fig_map.update_layout(
                    mapbox_style="open-street-map",
                    margin=dict(l=0, r=0, t=50, b=0),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.0,
                        xanchor="left",
                        x=0,
                        title_text="",  # Hide legend title
                        entrywidth=120,  # Fixed width per entry to force wrapping
                        entrywidthmode="pixels",
                    ),
                )

                _plotly_map(fig_map, key="topology_map_all")
            else:
                st.info("No GPS points available to map courses.")
            render_query_debug(centroids_q)

        else:
            # Per-course inspection view.
            st.caption(
                f"Showing topology for **{selected_course}** — representative points grouped by nine, hole, and section."
            )

            course_points_q = run_query(
                name="get_course_topology_map_points",
                sql=queries.get_course_topology_map_points(selected_course),
            )
            course_points = course_points_q.df
            if (
                course_points_q.ok
                and course_points is not None
                and not course_points.empty
            ):
                course_points = course_points.copy()
                # Treat nine_number as categorical for colour mapping.
                course_points["nine_number"] = course_points["nine_number"].astype(str)
                center, zoom = _map_center_zoom(course_points)

                def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
                    h = hex_color.lstrip("#")
                    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

                def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
                    return "#{:02x}{:02x}{:02x}".format(*rgb)

                def _blend_with_white(hex_color: str, t: float) -> str:
                    """Blend from white -> base colour (t in [0,1])."""
                    t = max(0.0, min(1.0, float(t)))
                    r, g, b = _hex_to_rgb(hex_color)
                    rr = int(round(255 * (1 - t) + r * t))
                    gg = int(round(255 * (1 - t) + g * t))
                    bb = int(round(255 * (1 - t) + b * t))
                    return _rgb_to_hex((rr, gg, bb))

                # Label points as hole.section_index within the hole, ordered by section_number.
                # Example: hole 1 sections 1-3 -> 1.1, 1.2, 1.3
                course_points["section_order_in_hole"] = (
                    course_points.groupby("hole_number")["section_number"]
                    .rank(method="dense", ascending=True)
                    .astype(int)
                )
                course_points["sections_in_hole"] = course_points.groupby(
                    "hole_number"
                )["section_number"].transform("nunique")
                course_points["hole_section_label"] = (
                    course_points["hole_number"].astype(int).astype(str)
                    + "."
                    + course_points["section_order_in_hole"].astype(int).astype(str)
                )

                # Render with visible hole labels (text on each marker).
                # Using Graph Objects here gives better control over marker+text rendering.
                fig_course = go.Figure()

                # Stable legend order: numeric nines first.
                nine_values = sorted(
                    course_points["nine_number"].unique().tolist(),
                    key=lambda x: int(x) if str(x).isdigit() else 999,
                )
                for idx, nine_num in enumerate(nine_values):
                    nine_df = course_points[
                        course_points["nine_number"] == nine_num
                    ].copy()
                    # Keep sizes readable and bounded.
                    marker_sizes = (
                        nine_df["event_count"].clip(lower=1) ** 0.5
                    ) * 2.0 + 10
                    marker_sizes = marker_sizes.clip(lower=10, upper=26)

                    # Progressive monochrome per hole: light for x.1 -> darker for x.n
                    # We keep colours within a nine on the same hue, but vary lightness.
                    base_hex = COLOR_SEQUENCE[idx % len(COLOR_SEQUENCE)]
                    denom = (nine_df["sections_in_hole"] - 1).replace(0, 1)
                    # t in [0.45, 1.0] so labels remain readable (avoid near-white).
                    t = 0.45 + 0.55 * ((nine_df["section_order_in_hole"] - 1) / denom)
                    marker_colors = [_blend_with_white(base_hex, v) for v in t.tolist()]

                    fig_course.add_trace(
                        go.Scattermapbox(
                            lat=nine_df["latitude"],
                            lon=nine_df["longitude"],
                            mode="markers+text",
                            text=nine_df["hole_section_label"],
                            textposition="middle center",
                            textfont=dict(size=11, color="white"),
                            marker=dict(
                                size=marker_sizes,
                                color=marker_colors,
                                opacity=0.8,
                            ),
                            name=f"nine {nine_num}",
                            customdata=nine_df[
                                [
                                    "hole_number",
                                    "section_number",
                                    "event_count",
                                    "projected_pct",
                                ]
                            ].values,
                            hovertemplate=(
                                "<b>hole %{customdata[0]}</b><br>"
                                "section: %{customdata[1]}<br>"
                                "events: %{customdata[2]}<br>"
                                "projected: %{customdata[3]}%<extra></extra>"
                            ),
                        )
                    )

                fig_course.update_layout(
                    mapbox_style="open-street-map",
                    mapbox_center=center,
                    mapbox_zoom=zoom,
                    margin=dict(l=0, r=0, t=50, b=0),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.0,
                        xanchor="left",
                        x=0,
                        title_text="",
                        entrywidth=80,
                        entrywidthmode="pixels",
                    ),
                    height=450,
                )
                _plotly_map(fig_course, key="topology_map_course")
            else:
                st.info("No GPS points available for this course.")
            render_query_debug(course_points_q)

        st.divider()

        # ---------------------------------------------------------------------
        # 4) Section ranges
        # ---------------------------------------------------------------------
        st.subheader("Section ranges")
        st.markdown(
            "Section ranges for courses, grouped by nine. When viewing all courses, each course uses a consistent colour."
        )

        # Visual: section range timeline (all courses or selected course)
        if selected_course == "All courses":
            # Show all courses' topology
            all_topo = topology.sort_values(["course_id", "nine_number"]).copy()

            # Calculate total sections per course for sorting
            all_topo["sections_in_nine"] = (
                all_topo["section_end"] - all_topo["section_start"] + 1
            )
            course_total_sections = (
                all_topo.groupby("course_id")["sections_in_nine"]
                .sum()
                .reset_index()
                .rename(columns={"sections_in_nine": "total_sections"})
            )

            # Merge total sections back and sort by total sections (ascending - lowest first)
            all_topo = all_topo.merge(course_total_sections, on="course_id", how="left")
            all_topo = all_topo.sort_values(
                ["total_sections", "course_id", "nine_number"],
                ascending=[True, True, True],
            )
            # Reverse the dataframe so lowest total sections appear at the top of the chart
            # (In horizontal bar charts, last row in dataframe = top of chart)
            all_topo = all_topo.iloc[::-1].reset_index(drop=True)

            fig_ranges = go.Figure()
            for i, (_, row) in enumerate(all_topo.iterrows()):
                start = int(row["section_start"])
                end = int(row["section_end"])
                length = end - start + 1
                label = f"{row['course_id']} - nine {int(row['nine_number'])}: {row['unit_name']}"
                fig_ranges.add_trace(
                    go.Bar(
                        y=[label],
                        x=[length],
                        base=[start],
                        orientation="h",
                        marker_color=course_color_map.get(
                            str(row["course_id"]), COLOR_SECONDARY
                        ),
                        text=[f"{start}–{end}"],
                        textposition="inside",
                        hovertemplate="<b>%{y}</b><br>section_start: %{base}<br>sections_in_nine: %{x}<extra></extra>",
                        showlegend=False,
                    )
                )

            fig_ranges.update_layout(
                height=max(240, 60 * len(all_topo)),
                margin=dict(l=20, r=20, t=40, b=40),
                xaxis_title="section_number",
                yaxis_title="",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                title="Nine boundaries for all courses (section ranges)",
                xaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(0,0,0,0.1)",
                    gridwidth=1,
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(0,0,0,0.1)",
                    gridwidth=1,
                ),
            )
            st.plotly_chart(fig_ranges, use_container_width=True)

            with st.expander("📋 View nine boundaries table", expanded=False):
                # Remove temporary columns before displaying
                all_topo_display = all_topo.drop(
                    columns=["sections_in_nine", "total_sections"], errors="ignore"
                )
                display_df(all_topo_display)
        else:
            course_topo = topology[topology["course_id"] == selected_course].copy()
            course_topo = course_topo.sort_values("nine_number")
            course_color = course_color_map.get(str(selected_course), COLOR_SECONDARY)

            fig_ranges = go.Figure()
            for i, (_, row) in enumerate(course_topo.iterrows()):
                start = int(row["section_start"])
                end = int(row["section_end"])
                length = end - start + 1
                label = f"nine {int(row['nine_number'])}: {row['unit_name']}"
                fig_ranges.add_trace(
                    go.Bar(
                        y=[label],
                        x=[length],
                        base=[start],
                        orientation="h",
                        marker_color=course_color,
                        text=[f"{start}–{end}"],
                        textposition="inside",
                        hovertemplate="<b>%{y}</b><br>section_start: %{base}<br>sections_in_nine: %{x}<extra></extra>",
                        showlegend=False,
                    )
                )

            fig_ranges.update_layout(
                height=max(240, 60 * len(course_topo)),
                margin=dict(l=20, r=20, t=40, b=40),
                xaxis_title="section_number",
                yaxis_title="",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                title=f"Nine boundaries for {selected_course} (section ranges)",
                xaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(0,0,0,0.1)",
                    gridwidth=1,
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(0,0,0,0.1)",
                    gridwidth=1,
                ),
            )
            st.plotly_chart(fig_ranges, use_container_width=True)

            with st.expander("📋 View nine boundaries table", expanded=False):
                display_df(course_topo)

    except Exception as e:
        st.error(f"Could not load topology: {e}")

    st.divider()

    # Sections per hole
    st.subheader("Sections per hole")
    st.markdown(
        """
    Sections-per-hole helps validate whether section numbering behaves like a standard layout
    (stable sections per hole) or a complex layout (loops or multi-unit facilities).
    """
    )

    try:
        # Prefer the precomputed dimension table if present (faster, and matches pipeline outputs).
        # Fallback to an on-the-fly aggregation if the dimension hasn't been generated yet.
        sph_dim_q = run_query(
            name="SECTIONS_PER_HOLE_DIM", sql=queries.SECTIONS_PER_HOLE_DIM
        )
        sph_q = sph_dim_q
        if not sph_dim_q.ok:
            sph_q = run_query(name="SECTIONS_PER_HOLE", sql=queries.SECTIONS_PER_HOLE)

        sections_per_hole = sph_q.df
        if sph_q.ok and sections_per_hole is not None and not sections_per_hole.empty:
            sections_per_hole = sections_per_hole.copy()
            sections_per_hole["course_id"] = sections_per_hole["course_id"].astype(str)

            # Get course color map for all courses in the data
            sph_course_ids = sorted(sections_per_hole["course_id"].unique().tolist())
            sph_course_color_map = get_course_color_map(sph_course_ids)

            if (
                "sections_count" in sections_per_hole.columns
                and "hole_number" in sections_per_hole.columns
            ):
                if selected_course == "All courses":
                    # Show all courses with grouped bars
                    fig_sph = go.Figure()

                    # Get all unique hole numbers across all courses
                    all_holes = sorted(
                        sections_per_hole["hole_number"].unique().tolist()
                    )

                    # Add a trace for each course
                    for course_id in sph_course_ids:
                        course_data = sections_per_hole[
                            sections_per_hole["course_id"] == course_id
                        ].copy()
                        course_data = course_data.sort_values("hole_number")

                        # Create a mapping of hole_number to sections_count
                        hole_to_count = dict(
                            zip(
                                course_data["hole_number"],
                                course_data["sections_count"],
                            )
                        )

                        # Build the y values for all holes (fill missing holes with 0)
                        y_values = [hole_to_count.get(hole, 0) for hole in all_holes]

                        course_color = sph_course_color_map.get(
                            course_id, COLOR_SECONDARY
                        )

                        fig_sph.add_trace(
                            go.Bar(
                                name=course_id,
                                x=all_holes,
                                y=y_values,
                                marker_color=course_color,
                                legendgroup=course_id,
                            )
                        )

                    fig_sph.update_layout(
                        barmode="group",
                        height=350,
                        margin=dict(l=20, r=20, t=40, b=40),
                        xaxis_title="hole_number",
                        yaxis_title="sections_count",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        title="Sections per hole for all courses",
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1,
                        ),
                    )
                    st.plotly_chart(fig_sph, use_container_width=True)
                else:
                    # Show single course
                    sph_course = sections_per_hole[
                        sections_per_hole["course_id"] == selected_course
                    ].copy()
                    sph_course = sph_course.sort_values(["hole_number"])

                    course_color = sph_course_color_map.get(
                        selected_course, COLOR_SECONDARY
                    )

                    # Monochromatic bar colours for the selected course (progressive shades).
                    def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
                        h = hex_color.lstrip("#")
                        return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

                    def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
                        return "#{:02x}{:02x}{:02x}".format(*rgb)

                    def _blend_with_white(hex_color: str, t: float) -> str:
                        """Blend from white -> base colour (t in [0,1])."""
                        t = max(0.0, min(1.0, float(t)))
                        r, g, b = _hex_to_rgb(hex_color)
                        rr = int(round(255 * (1 - t) + r * t))
                        gg = int(round(255 * (1 - t) + g * t))
                        bb = int(round(255 * (1 - t) + b * t))
                        return _rgb_to_hex((rr, gg, bb))

                    # Progressive monochrome shades across holes (light -> dark),
                    # using the selected course base colour (from the sidebar legend).
                    n = max(len(sph_course), 1)
                    # t in [0.45, 1.0] to avoid near-white.
                    shades = [
                        _blend_with_white(
                            course_color, 0.45 + 0.55 * (i / max(n - 1, 1))
                        )
                        for i in range(n)
                    ]

                    fig_sph = go.Figure(
                        data=[
                            go.Bar(
                                x=sph_course["hole_number"],
                                y=sph_course["sections_count"],
                                marker=dict(color=shades),
                            )
                        ]
                    )
                    fig_sph.update_layout(
                        height=260,
                        margin=dict(l=20, r=20, t=40, b=40),
                        xaxis_title="hole_number",
                        yaxis_title="sections_count",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        title=f"Sections per hole for {selected_course}",
                    )
                    st.plotly_chart(fig_sph, use_container_width=True)

            with st.expander("📋 View sections per hole table", expanded=False):
                if selected_course == "All courses":
                    display_df(
                        sections_per_hole.sort_values(["course_id", "hole_number"])
                    )
                else:
                    sph_course_table = sections_per_hole[
                        sections_per_hole["course_id"] == selected_course
                    ].copy()
                    sph_course_table = sph_course_table.sort_values(["hole_number"])
                    display_df(sph_course_table)
            render_query_debug(sph_q)
        else:
            st.info(
                "No sections-per-hole data available. Run the generation script first."
            )
            render_query_debug(sph_q)
    except Exception as e:
        st.error(f"Could not load sections per hole: {e}")


# =============================================================================
# PAGE: PACE ANALYSIS
# =============================================================================


def render_pace_analysis():
    """Pace Analysis page - identify bottlenecks and compare pace across holes/nines."""
    st.header("Pace analysis")
    st.markdown(
        """
    Identify bottleneck holes and analyze pace patterns across your courses.
    A hole is considered a **bottleneck** if its average pace is >15% above the course average.
    """
    )

    # Get course list for selector
    course_filter = st.session_state.get("course_filter", "All courses")

    st.divider()

    # -------------------------------------------------------------------------
    # Bottleneck Summary (All Courses)
    # -------------------------------------------------------------------------
    st.subheader("Bottleneck summary")
    st.markdown("Overview of bottleneck holes across all courses.")

    bottleneck_q = run_query(name="BOTTLENECK_SUMMARY", sql=queries.BOTTLENECK_SUMMARY)
    if bottleneck_q.ok and bottleneck_q.df is not None and not bottleneck_q.df.empty:
        bottleneck_df = bottleneck_q.df.copy()
        bottleneck_df["course_id"] = bottleneck_df["course_id"].astype(str)

        # Show summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            total_bottlenecks = bottleneck_df["bottleneck_holes"].sum()
            st.metric("Total bottleneck holes", int(total_bottlenecks))
        with col2:
            total_fast = bottleneck_df["fast_holes"].sum()
            st.metric("Total fast holes", int(total_fast))
        with col3:
            avg_pace = bottleneck_df["course_avg_pace"].mean()
            st.metric("Avg course pace", f"{avg_pace:.1f} min")
        with col4:
            worst_course = bottleneck_df.loc[
                bottleneck_df["bottleneck_holes"].idxmax(), "course_id"
            ]
            st.metric("Most bottlenecks", worst_course)

        # Show table
        display_df(bottleneck_df)

    render_query_debug(bottleneck_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Per-Course Pace Analysis
    # -------------------------------------------------------------------------
    st.subheader("Pace by hole")
    st.markdown(
        "Select a course to see pace analysis by hole. "
        "Bottleneck holes are highlighted in red, fast holes in green."
    )

    # Course selector for this section
    try:
        courses_df = execute_query(
            "SELECT DISTINCT course_id FROM iceberg.silver.fact_telemetry_event ORDER BY course_id"
        )
        if courses_df is not None and not courses_df.empty:
            course_options = courses_df["course_id"].astype(str).tolist()
        else:
            course_options = []
    except Exception:
        course_options = []

    if not course_options:
        st.warning("No courses available for pace analysis.")
        return

    selected_course = st.selectbox(
        "Select course for pace analysis",
        options=course_options,
        key="pace_analysis_course",
    )

    if selected_course:
        pace_q = run_query(
            name=f"PACE_BY_HOLE_{selected_course}",
            sql=queries.get_pace_by_hole_for_course(selected_course),
        )

        if pace_q.ok and pace_q.df is not None and not pace_q.df.empty:
            pace_df = pace_q.df.copy()

            # Get course average for reference line
            course_avg = pace_df["course_avg_pace"].iloc[0] if len(pace_df) > 0 else 0

            # Create color mapping based on hole category
            color_map = {
                "bottleneck": COLOR_CRITICAL,
                "fast": COLOR_GOOD,
                "normal": COLOR_INFO,
            }
            colors = [
                color_map.get(cat, COLOR_INFO) for cat in pace_df["hole_category"]
            ]

            # Create bar chart
            fig = go.Figure()

            fig.add_trace(
                go.Bar(
                    x=pace_df["hole_number"],
                    y=pace_df["avg_pace"],
                    marker_color=colors,
                    text=[f"{v:.1f}" for v in pace_df["avg_pace"]],
                    textposition="outside",
                    hovertemplate=(
                        "<b>Hole %{x}</b><br>"
                        "Avg pace: %{y:.1f} min<br>"
                        "Samples: %{customdata[0]} rounds<br>"
                        "Category: %{customdata[1]}<extra></extra>"
                    ),
                    customdata=list(
                        zip(pace_df["sample_rounds"], pace_df["hole_category"])
                    ),
                )
            )

            # Add course average line
            fig.add_hline(
                y=course_avg,
                line_dash="dash",
                line_color=COLOR_WARNING,
                annotation_text=f"Course avg: {course_avg:.1f} min",
                annotation_position="top right",
            )

            # Add bottleneck threshold line (15% above average)
            bottleneck_threshold = course_avg * 1.15
            fig.add_hline(
                y=bottleneck_threshold,
                line_dash="dot",
                line_color=COLOR_CRITICAL,
                annotation_text=f"Bottleneck threshold: {bottleneck_threshold:.1f} min",
                annotation_position="bottom right",
            )

            fig.update_layout(
                height=400,
                margin=dict(l=20, r=20, t=60, b=40),
                xaxis_title="Hole number",
                yaxis_title="Average pace (minutes)",
                xaxis=dict(dtick=1),
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                title=f"Pace by hole — {selected_course}",
            )

            st.plotly_chart(fig, use_container_width=True)

            # Legend explanation
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(
                    f'<span style="color:{COLOR_CRITICAL}">●</span> **Bottleneck** (>15% above avg)',
                    unsafe_allow_html=True,
                )
            with col2:
                st.markdown(
                    f'<span style="color:{COLOR_GOOD}">●</span> **Fast** (<15% below avg)',
                    unsafe_allow_html=True,
                )
            with col3:
                st.markdown(
                    f'<span style="color:{COLOR_INFO}">●</span> **Normal**',
                    unsafe_allow_html=True,
                )

            # Show detailed table
            with st.expander("View pace details by hole"):
                display_df(pace_df)

        render_query_debug(pace_q)

    st.divider()

    # -------------------------------------------------------------------------
    # 9-Hole Loop Analysis (American Falls style)
    # -------------------------------------------------------------------------
    st.subheader("9-hole loop analysis")
    st.markdown(
        """
    For 9-hole courses that allow 18-hole rounds (playing the same holes twice),
    compare pace on each hole between the **first nine** (`nine_number=1`) and 
    **second nine** (`nine_number=2`).
    
    This analysis filters for 18-hole rounds only (`is_nine_hole=FALSE`), excluding 
    single 9-hole rounds. This lets you see if players slow down on the second pass
    through the same holes (fatigue effect) or speed up (familiarity effect).
    """
    )

    # Get loop courses from course profile
    loop_courses_q = run_query(name="LOOP_COURSES", sql=queries.LOOP_COURSES)
    loop_course_options = []
    if (
        loop_courses_q.ok
        and loop_courses_q.df is not None
        and not loop_courses_q.df.empty
    ):
        loop_course_options = loop_courses_q.df["course_id"].astype(str).tolist()

    if not loop_course_options:
        st.info(
            "No loop courses configured. Loop courses are 9-hole courses that support 18-hole rounds. "
            "Configure them in `dim_course_profile.csv` with `is_loop_course=true`."
        )
        loop_course = None
    else:
        # Course selector for loop analysis (only shows loop courses)
        loop_course = st.selectbox(
            "Select loop course",
            options=loop_course_options,
            key="loop_analysis_course",
            help="Only courses marked as loop courses in dim_course_profile are shown here.",
        )

    # Hole selector
    hole_options = list(range(1, 10))  # Holes 1-9 for a 9-hole course
    selected_hole = st.selectbox(
        "Select hole to compare",
        options=hole_options,
        index=4,  # Default to hole 5
        key="loop_analysis_hole",
    )

    if loop_course:
        # Get the nine loop comparison data
        loop_q = run_query(
            name=f"NINE_LOOP_PACE_{loop_course}",
            sql=queries.get_nine_loop_pace_comparison(loop_course),
        )

        if loop_q.ok and loop_q.df is not None and not loop_q.df.empty:
            loop_df = loop_q.df.copy()

            # Check if we have data for both passes
            passes = loop_df["pass_number"].unique()
            if len(passes) < 2:
                st.warning(
                    f"Not enough 18-hole round data for {loop_course}. "
                    "This analysis requires rounds that played the course twice."
                )
            else:
                # Pivot for comparison
                first_nine_data = loop_df[loop_df["pass_number"] == "first_nine"].copy()
                second_nine_data = loop_df[
                    loop_df["pass_number"] == "second_nine"
                ].copy()

                # Merge to calculate delta
                delta_df = first_nine_data.merge(
                    second_nine_data,
                    on="hole_number",
                    suffixes=("_first", "_second"),
                )
                delta_df["pace_delta"] = (
                    delta_df["avg_pace_second"] - delta_df["avg_pace_first"]
                )
                delta_df["pct_delta"] = (
                    delta_df["pace_delta"] / delta_df["avg_pace_first"] * 100
                )

                # Color bars: red if slower (positive delta), green if faster (negative)
                colors = [
                    COLOR_CRITICAL if d > 0 else COLOR_GOOD
                    for d in delta_df["pace_delta"]
                ]

                # Create delta bar chart
                fig = go.Figure()

                fig.add_trace(
                    go.Bar(
                        x=delta_df["hole_number"],
                        y=delta_df["pace_delta"],
                        marker_color=colors,
                        text=[f"{d:+.1f}" for d in delta_df["pace_delta"]],
                        textposition="outside",
                        hovertemplate=(
                            "<b>Hole %{x}</b><br>"
                            "Delta: %{y:+.1f} min<br>"
                            "First nine: %{customdata[0]:.1f} min<br>"
                            "Second nine: %{customdata[1]:.1f} min<extra></extra>"
                        ),
                        customdata=list(
                            zip(delta_df["avg_pace_first"], delta_df["avg_pace_second"])
                        ),
                    )
                )

                # Add zero line
                fig.add_hline(y=0, line_dash="solid", line_color="gray", line_width=1)

                fig.update_layout(
                    height=400,
                    margin=dict(l=20, r=20, t=60, b=40),
                    xaxis_title="Hole number",
                    yaxis_title="Pace delta (second nine − first nine)",
                    xaxis=dict(dtick=1),
                    showlegend=False,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    title=f"Pace difference by hole — {loop_course} (18-hole rounds only)",
                )

                st.plotly_chart(fig, use_container_width=True)

                # Legend explanation
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(
                        f'<span style="color:{COLOR_CRITICAL}">●</span> **Positive** = slower on second nine (fatigue?)',
                        unsafe_allow_html=True,
                    )
                with col2:
                    st.markdown(
                        f'<span style="color:{COLOR_GOOD}">●</span> **Negative** = faster on second nine (familiarity?)',
                        unsafe_allow_html=True,
                    )

                st.divider()

                # Show specific hole comparison with selector
                st.markdown(f"### Hole {selected_hole} detailed comparison")

                hole_data = delta_df[delta_df["hole_number"] == selected_hole]

                if not hole_data.empty:
                    row = hole_data.iloc[0]
                    first_pace = row["avg_pace_first"]
                    second_pace = row["avg_pace_second"]
                    diff = row["pace_delta"]
                    pct_diff = row["pct_delta"]
                    first_rounds = row["sample_rounds_first"]
                    second_rounds = row["sample_rounds_second"]

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric(
                            f"Hole {selected_hole} — First nine",
                            f"{first_pace:.1f} min",
                            help=f"Based on {int(first_rounds)} rounds",
                        )
                    with col2:
                        st.metric(
                            f"Hole {selected_hole} — Second nine",
                            f"{second_pace:.1f} min",
                            delta=f"{diff:+.1f} min ({pct_diff:+.1f}%)",
                            delta_color="inverse",  # Red if slower (positive)
                            help=f"Based on {int(second_rounds)} rounds",
                        )
                    with col3:
                        if diff > 0.5:
                            st.markdown(
                                f"**Finding:** Hole {selected_hole} plays **slower** on the second nine "
                                f"(+{diff:.1f} min). This could indicate fatigue or changing course conditions."
                            )
                        elif diff < -0.5:
                            st.markdown(
                                f"**Finding:** Hole {selected_hole} plays **faster** on the second nine "
                                f"({diff:.1f} min). Players may be more familiar with the hole."
                            )
                        else:
                            st.markdown(
                                f"**Finding:** Hole {selected_hole} pace is **consistent** between nines "
                                f"(difference: {abs(diff):.1f} min)."
                            )
                else:
                    st.info(f"No data available for hole {selected_hole} comparison.")

                # Show full comparison table
                with st.expander("View full comparison data"):
                    display_df(loop_df)

        else:
            st.warning(
                f"No 18-hole round data found for {loop_course}. "
                "This course may not have enough rounds that played the full 18 holes."
            )

        render_query_debug(loop_q)

    st.divider()

    # -------------------------------------------------------------------------
    # Pace by Section (Granular)
    # -------------------------------------------------------------------------
    st.subheader("Pace by section (granular)")
    st.markdown("For deeper analysis, view pace at the section level within each hole.")

    section_course = st.selectbox(
        "Select course for section analysis",
        options=course_options,
        key="section_analysis_course",
    )

    if section_course:
        section_q = run_query(
            name=f"PACE_BY_SECTION_{section_course}",
            sql=queries.get_pace_by_section_for_course(section_course),
        )

        if section_q.ok and section_q.df is not None and not section_q.df.empty:
            section_df = section_q.df.copy()

            # Create heatmap of pace by hole and section
            pivot = section_df.pivot_table(
                index="hole_number",
                columns="section_number",
                values="avg_pace",
                aggfunc="first",
            )

            fig = go.Figure(
                data=go.Heatmap(
                    z=pivot.values,
                    x=[f"S{int(c)}" for c in pivot.columns],
                    y=[f"H{int(h)}" for h in pivot.index],
                    colorscale="RdYlGn_r",  # Red = slow, Green = fast
                    text=[
                        [f"{v:.1f}" if not pd.isna(v) else "" for v in row]
                        for row in pivot.values
                    ],
                    texttemplate="%{text}",
                    textfont={"size": 9},
                    hoverongaps=False,
                    colorbar=dict(title="Pace (min)"),
                )
            )

            fig.update_layout(
                height=400,
                margin=dict(l=20, r=20, t=60, b=40),
                xaxis_title="Section",
                yaxis_title="Hole",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                title=f"Pace heatmap by hole × section — {section_course}",
            )

            st.plotly_chart(fig, use_container_width=True)

            with st.expander("View section pace details"):
                display_df(section_df)

        render_query_debug(section_q)


# =============================================================================
# PAGE: GLOBAL METRICS
# =============================================================================


def render_global_metrics():
    """Global Metrics page - demonstrating cross-course insights.

    This page showcases the value of having all course data in a single lakehouse
    rather than siloed MongoDB databases. These insights are IMPOSSIBLE to generate
    without unified data.
    """
    st.header("Global metrics")
    st.markdown(
        """
    **The power of unified data.** These insights are only possible because all course
    data lives in a single lakehouse. With siloed MongoDB databases, you'd need to
    manually export, transform, and combine data from each course — a process that's
    slow, error-prone, and often impossible in practice.
    """
    )

    st.divider()

    # -------------------------------------------------------------------------
    # Global Overview KPIs
    # -------------------------------------------------------------------------
    st.subheader("Portfolio overview")
    st.markdown(
        "Aggregate statistics across your entire course portfolio — computed in a single query."
    )

    global_overview_q = run_query(name="GLOBAL_OVERVIEW", sql=queries.GLOBAL_OVERVIEW)
    if (
        global_overview_q.ok
        and global_overview_q.df is not None
        and not global_overview_q.df.empty
    ):
        row = global_overview_q.df.iloc[0]

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total courses", f"{int(row.get('total_courses', 0)):,}")
        with col2:
            st.metric("Total rounds", f"{int(row.get('total_rounds', 0)):,}")
        with col3:
            st.metric("Total events", f"{int(row.get('total_events', 0)):,}")
        with col4:
            st.metric("Unique devices", f"{int(row.get('unique_devices', 0)):,}")
        with col5:
            st.metric("Playing days", f"{int(row.get('total_playing_days', 0)):,}")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            earliest = row.get("earliest_date")
            st.metric("Earliest data", str(earliest)[:10] if earliest else "N/A")
        with col2:
            latest = row.get("latest_date")
            st.metric("Latest data", str(latest)[:10] if latest else "N/A")
        with col3:
            st.metric("Global avg pace", f"{row.get('global_avg_pace', 0):.1f} min")
        with col4:
            st.metric("Global avg battery", f"{row.get('global_avg_battery', 0):.1f}%")

    render_query_debug(global_overview_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Pace Benchmarking
    # -------------------------------------------------------------------------
    st.subheader("Pace benchmarking")
    st.markdown(
        "Compare pace of play across all courses. "
        "**This cross-course comparison was impossible with siloed databases.**"
    )

    pace_q = run_query(
        name="GLOBAL_PACE_COMPARISON", sql=queries.GLOBAL_PACE_COMPARISON
    )
    if pace_q.ok and pace_q.df is not None and not pace_q.df.empty:
        pace_df = pace_q.df.copy()
        pace_df["course_id"] = pace_df["course_id"].astype(str)

        # Get course color map
        course_ids = pace_df["course_id"].tolist()
        course_color_map = get_course_color_map(course_ids)

        fig = go.Figure()

        # Add bars for average pace
        fig.add_trace(
            go.Bar(
                x=pace_df["course_id"],
                y=pace_df["avg_pace"],
                name="Average pace",
                marker_color=[
                    course_color_map.get(c, COLOR_PRIMARY) for c in pace_df["course_id"]
                ],
                text=[f"{v:.1f}" for v in pace_df["avg_pace"]],
                textposition="outside",
            )
        )

        # Add global average line
        global_avg = pace_df["avg_pace"].mean()
        fig.add_hline(
            y=global_avg,
            line_dash="dash",
            line_color=COLOR_WARNING,
            annotation_text=f"Global avg: {global_avg:.1f}",
            annotation_position="top right",
        )

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=60, b=40),
            xaxis_title="Course",
            yaxis_title="Average pace (minutes)",
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Average pace by course (with global benchmark)",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Show detailed table
        with st.expander("View pace details"):
            display_df(pace_df)

    render_query_debug(pace_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Round Duration Comparison
    # -------------------------------------------------------------------------
    st.subheader("Round duration comparison")
    st.markdown(
        "Which courses play faster? Compare round durations across your portfolio."
    )

    duration_q = run_query(
        name="GLOBAL_ROUND_DURATION_COMPARISON",
        sql=queries.GLOBAL_ROUND_DURATION_COMPARISON,
    )
    if duration_q.ok and duration_q.df is not None and not duration_q.df.empty:
        duration_df = duration_q.df.copy()
        duration_df["course_id"] = duration_df["course_id"].astype(str)

        course_ids = duration_df["course_id"].tolist()
        course_color_map = get_course_color_map(course_ids)

        fig = go.Figure()

        # Box-like visualization with min, avg, max
        for _, row in duration_df.iterrows():
            course = row["course_id"]
            color = course_color_map.get(course, COLOR_PRIMARY)

            # Add error bar showing range
            fig.add_trace(
                go.Scatter(
                    x=[course],
                    y=[row["avg_duration_min"]],
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=[row["max_duration_min"] - row["avg_duration_min"]],
                        arrayminus=[row["avg_duration_min"] - row["min_duration_min"]],
                        color=color,
                    ),
                    mode="markers",
                    marker=dict(size=12, color=color),
                    name=course,
                    showlegend=False,
                )
            )

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=60, b=40),
            xaxis_title="Course",
            yaxis_title="Round duration (minutes)",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Round duration range by course (min–avg–max)",
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("View duration details"):
            display_df(duration_df)

    render_query_debug(duration_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Data Quality Ranking
    # -------------------------------------------------------------------------
    st.subheader("Data quality ranking")
    st.markdown(
        "Identify which courses need attention. "
        "Unified data lets you spot patterns across your entire portfolio."
    )

    quality_q = run_query(
        name="GLOBAL_DATA_QUALITY_RANKING", sql=queries.GLOBAL_DATA_QUALITY_RANKING
    )
    if quality_q.ok and quality_q.df is not None and not quality_q.df.empty:
        quality_df = quality_q.df.copy()
        quality_df["course_id"] = quality_df["course_id"].astype(str)

        course_ids = quality_df["course_id"].tolist()
        course_color_map = get_course_color_map(course_ids)

        # Create a grouped bar chart for quality metrics
        fig = go.Figure()

        metrics = [
            "pace_completeness",
            "gps_completeness",
            "hole_completeness",
            "timestamp_completeness",
        ]
        metric_labels = ["Pace", "GPS", "Hole #", "Timestamp"]

        for metric, label in zip(metrics, metric_labels):
            fig.add_trace(
                go.Bar(
                    x=quality_df["course_id"],
                    y=quality_df[metric],
                    name=label,
                )
            )

        fig.update_layout(
            barmode="group",
            height=350,
            margin=dict(l=20, r=20, t=60, b=40),
            xaxis_title="Course",
            yaxis_title="Completeness (%)",
            yaxis_range=[0, 105],
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Data completeness by course (higher is better)",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Show ranking table
        st.markdown("**Quality ranking**")
        ranking_df = quality_df[
            ["quality_rank", "course_id", "avg_quality_score", "total_events"]
        ].copy()
        ranking_df.columns = ["Rank", "Course", "Avg quality score", "Total events"]
        display_df(ranking_df)

    render_query_debug(quality_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Weekday Heatmap
    # -------------------------------------------------------------------------
    st.subheader("Play patterns by weekday")
    st.markdown(
        "When do golfers play across your portfolio? "
        "Discover global patterns in play behavior."
    )

    weekday_q = run_query(
        name="GLOBAL_WEEKDAY_HEATMAP", sql=queries.GLOBAL_WEEKDAY_HEATMAP
    )
    if weekday_q.ok and weekday_q.df is not None and not weekday_q.df.empty:
        weekday_df = weekday_q.df.copy()
        weekday_df["course_id"] = weekday_df["course_id"].astype(str)

        # Pivot for heatmap
        pivot = weekday_df.pivot_table(
            index="course_id",
            columns="event_weekday",
            values="round_count",
            aggfunc="sum",
            fill_value=0,
        )

        # Reorder columns to start with Monday (1) through Sunday (7)
        weekday_order = [1, 2, 3, 4, 5, 6, 7]
        weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        pivot = pivot.reindex(columns=[c for c in weekday_order if c in pivot.columns])

        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values,
                x=weekday_names[: len(pivot.columns)],
                y=pivot.index.tolist(),
                colorscale="Blues",
                text=pivot.values,
                texttemplate="%{text}",
                textfont={"size": 10},
                hoverongaps=False,
            )
        )

        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=60, b=40),
            xaxis_title="Day of week",
            yaxis_title="Course",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Rounds by weekday (heatmap)",
        )
        st.plotly_chart(fig, use_container_width=True)

    render_query_debug(weekday_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Device Fleet Overview
    # -------------------------------------------------------------------------
    st.subheader("Device fleet overview")
    st.markdown(
        "Understand device distribution and health across your entire portfolio."
    )

    device_q = run_query(name="GLOBAL_DEVICE_FLEET", sql=queries.GLOBAL_DEVICE_FLEET)
    if device_q.ok and device_q.df is not None and not device_q.df.empty:
        device_df = device_q.df.copy()
        device_df["course_id"] = device_df["course_id"].astype(str)

        col1, col2 = st.columns(2)

        with col1:
            # Devices per course
            course_ids = device_df["course_id"].tolist()
            course_color_map = get_course_color_map(course_ids)

            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=device_df["course_id"],
                    y=device_df["unique_devices"],
                    marker_color=[
                        course_color_map.get(c, COLOR_PRIMARY)
                        for c in device_df["course_id"]
                    ],
                    text=device_df["unique_devices"],
                    textposition="outside",
                )
            )
            fig.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=60, b=40),
                xaxis_title="Course",
                yaxis_title="Unique devices",
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                title="Device count by course",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Rounds per device efficiency
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=device_df["course_id"],
                    y=device_df["rounds_per_device"],
                    marker_color=[
                        course_color_map.get(c, COLOR_PRIMARY)
                        for c in device_df["course_id"]
                    ],
                    text=[f"{v:.1f}" for v in device_df["rounds_per_device"]],
                    textposition="outside",
                )
            )
            fig.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=60, b=40),
                xaxis_title="Course",
                yaxis_title="Rounds per device",
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                title="Device utilization (rounds/device)",
            )
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("View device fleet details"):
            display_df(device_df)

    render_query_debug(device_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Device Health (Battery & Projected Events)
    # -------------------------------------------------------------------------
    st.subheader("Device health")
    st.markdown(
        "Battery levels and projected events across all courses. "
        "Projected events occur when devices had no signal or when the battery died."
    )

    devices_q = run_query(name="DEVICE_STATS", sql=queries.DEVICE_STATS)
    devices_df = devices_q.df
    if devices_q.ok and devices_df is not None and not devices_df.empty:
        # Create battery health visualization
        fig_battery = go.Figure()

        # Add average battery bar
        fig_battery.add_trace(
            go.Bar(
                name="avg_battery",
                x=devices_df["course_id"],
                y=devices_df["avg_battery"],
                marker_color=COLOR_GOOD,  # Green
                text=[f"{val:.1f}%" for val in devices_df["avg_battery"]],
                textposition="outside",
            )
        )

        # Add minimum battery bar
        fig_battery.add_trace(
            go.Bar(
                name="min_battery",
                x=devices_df["course_id"],
                y=devices_df["min_battery"],
                marker_color=COLOR_WARNING,  # Yellow/Amber
                text=[f"{val:.1f}%" for val in devices_df["min_battery"]],
                textposition="outside",
            )
        )

        fig_battery.update_layout(
            barmode="group",
            height=350,
            margin=dict(
                l=20, r=20, t=80, b=40
            ),  # Increased top margin for text outside bars
            xaxis_title="Course",
            yaxis_title="Battery percentage",
            yaxis_range=[0, 100],
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Battery levels by course",
        )

        st.plotly_chart(fig_battery, use_container_width=True)

    render_query_debug(devices_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Completion Rates
    # -------------------------------------------------------------------------
    st.subheader("Round completion rates")
    st.markdown(
        "Which courses have the highest round completion rates? "
        "Spot operational patterns across your portfolio."
    )

    completion_q = run_query(
        name="GLOBAL_COMPLETION_RATES", sql=queries.GLOBAL_COMPLETION_RATES
    )
    if completion_q.ok and completion_q.df is not None and not completion_q.df.empty:
        completion_df = completion_q.df.copy()
        completion_df["course_id"] = completion_df["course_id"].astype(str)

        course_ids = completion_df["course_id"].tolist()
        course_color_map = get_course_color_map(course_ids)

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=completion_df["course_id"],
                y=completion_df["completion_rate"],
                marker_color=[
                    course_color_map.get(c, COLOR_PRIMARY)
                    for c in completion_df["course_id"]
                ],
                text=[f"{v:.1f}%" for v in completion_df["completion_rate"]],
                textposition="outside",
            )
        )

        # Add global average line
        global_completion = completion_df["completion_rate"].mean()
        fig.add_hline(
            y=global_completion,
            line_dash="dash",
            line_color=COLOR_WARNING,
            annotation_text=f"Global avg: {global_completion:.1f}%",
            annotation_position="top right",
        )

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=60, b=40),
            xaxis_title="Course",
            yaxis_title="Completion rate (%)",
            yaxis_range=[0, 105],
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Round completion rate by course",
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("View completion details"):
            display_df(completion_df)

    render_query_debug(completion_q)
    st.divider()

    # -------------------------------------------------------------------------
    # Monthly Trends
    # -------------------------------------------------------------------------
    st.subheader("Monthly trends")
    st.markdown(
        "See seasonality patterns across your entire portfolio. "
        "Unified data reveals trends that span courses and time."
    )

    monthly_q = run_query(name="GLOBAL_MONTHLY_TREND", sql=queries.GLOBAL_MONTHLY_TREND)
    if monthly_q.ok and monthly_q.df is not None and not monthly_q.df.empty:
        monthly_df = monthly_q.df.copy()
        monthly_df["course_id"] = monthly_df["course_id"].astype(str)

        # Create year-month column for x-axis
        monthly_df["year_month"] = (
            monthly_df["event_year"].astype(str)
            + "-"
            + monthly_df["event_month"].astype(str).str.zfill(2)
        )

        course_ids = monthly_df["course_id"].unique().tolist()
        course_color_map = get_course_color_map(course_ids)

        fig = go.Figure()
        for course in sorted(course_ids):
            course_data = monthly_df[monthly_df["course_id"] == course]
            fig.add_trace(
                go.Scatter(
                    x=course_data["year_month"],
                    y=course_data["round_count"],
                    mode="lines+markers",
                    name=course,
                    line=dict(color=course_color_map.get(course, COLOR_PRIMARY)),
                    marker=dict(size=6),
                )
            )

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=60, b=40),
            xaxis_title="Month",
            yaxis_title="Rounds",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            title="Monthly round volume by course",
        )
        st.plotly_chart(fig, use_container_width=True)

    render_query_debug(monthly_q)

    # -------------------------------------------------------------------------
    # Value Proposition Summary
    # -------------------------------------------------------------------------
    st.divider()
    st.subheader("The lakehouse advantage")
    st.markdown(
        """
    **What you just saw would be impossible with siloed MongoDB databases:**

    1. **Single-query aggregates** — Total rounds, events, and devices across ALL courses in one query
    2. **Cross-course benchmarking** — Compare pace, duration, and quality metrics across your portfolio
    3. **Pattern discovery** — Spot weekday and seasonal trends that span multiple courses
    4. **Unified quality monitoring** — Rank courses by data quality to prioritize attention
    5. **Fleet-wide device analytics** — Understand device utilization across all locations

    With the legacy architecture, each of these insights would require:
    - Manual data exports from each MongoDB instance
    - Custom ETL scripts to normalize schemas
    - Spreadsheet gymnastics to combine and analyze
    - Hours or days of work vs. **milliseconds** with the lakehouse
    """
    )


# =============================================================================
# PAGE: INFRASTRUCTURE & PROJECTIONS
# =============================================================================


def render_infrastructure():
    """Infrastructure & Projections page - sizing, costs, and AWS estimates."""
    st.header("Infrastructure & projections")
    st.markdown(
        """
    Understand the current data footprint and project costs for scaling to 
    **650 courses × 7 years** of historical data on AWS.
    """
    )

    st.divider()

    # -------------------------------------------------------------------------
    # Current Pilot Data
    # -------------------------------------------------------------------------
    st.subheader("Current pilot data")

    # Hardcoded pilot file sizes (from ls -lh data/)
    pilot_files = [
        {
            "file": "americanfalls.rounds.csv",
            "size_mb": 13,
            "rows": 2949,
            "course": "americanfalls",
        },
        {
            "file": "bradshawfarmgc.rounds_0601_0715.csv",
            "size_mb": 60,
            "rows": 8048,
            "course": "bradshawfarmgc",
        },
        {
            "file": "bradshawfarmgc.rounds_0716_0831.csv",
            "size_mb": 66,
            "rows": 8389,
            "course": "bradshawfarmgc",
        },
        {
            "file": "erinhills.rounds.csv",
            "size_mb": 23,
            "rows": 3471,
            "course": "erinhills",
        },
        {
            "file": "indiancreek.rounds.csv",
            "size_mb": 10,
            "rows": 1684,
            "course": "indiancreek",
        },
        {
            "file": "pinehurst4.rounds.csv",
            "size_mb": 22,
            "rows": 3010,
            "course": "pinehurst4",
        },
    ]
    pilot_df = pd.DataFrame(pilot_files)

    total_raw_mb = pilot_df["size_mb"].sum()
    total_rounds = pilot_df["rows"].sum()
    num_courses = pilot_df["course"].nunique()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Pilot courses", num_courses)
    with col2:
        st.metric("Total rounds", f"{total_rounds:,}")
    with col3:
        st.metric("Raw CSV size", f"{total_raw_mb} MB")
    with col4:
        avg_mb_per_course = total_raw_mb / num_courses
        st.metric("Avg per course", f"{avg_mb_per_course:.1f} MB")

    # Show file details
    with st.expander("View pilot file details"):
        display_df(pilot_df)

    # Query actual Silver layer stats
    infra_q = run_query(name="INFRASTRUCTURE_STATS", sql=queries.INFRASTRUCTURE_STATS)
    if infra_q.ok and infra_q.df is not None and not infra_q.df.empty:
        row = infra_q.df.iloc[0]

        st.markdown("#### Silver layer (Iceberg)")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total events", f"{int(row.get('total_events', 0)):,}")
        with col2:
            st.metric("Real events", f"{int(row.get('real_events', 0)):,}")
        with col3:
            st.metric("Total rounds", f"{int(row.get('total_rounds', 0)):,}")
        with col4:
            st.metric(
                "Date range", f"{row.get('earliest_date')} → {row.get('latest_date')}"
            )

    render_query_debug(infra_q)

    # Events per course
    events_q = run_query(name="EVENTS_PER_COURSE", sql=queries.EVENTS_PER_COURSE)
    if events_q.ok and events_q.df is not None and not events_q.df.empty:
        with st.expander("View events per course"):
            display_df(events_q.df)

    st.divider()

    # -------------------------------------------------------------------------
    # Full-Scale Projections
    # -------------------------------------------------------------------------
    st.subheader("Full-scale projections")
    st.markdown(
        """
    Project storage and compute requirements for the full Tagmarshal deployment:
    **650 courses × 7 years** of historical data.
    """
    )

    # User inputs for projection
    col1, col2, col3 = st.columns(3)
    with col1:
        target_courses = st.number_input(
            "Target courses", value=650, min_value=1, max_value=2000
        )
    with col2:
        target_years = st.number_input(
            "Years of history", value=7, min_value=1, max_value=20
        )
    with col3:
        avg_rounds_per_course_year = st.number_input(
            "Avg rounds/course/year",
            value=5000,
            min_value=100,
            max_value=50000,
            help="Typical golf course sees 3,000-10,000 rounds per year",
        )

    # Calculate projections based on pilot data
    # Pilot: ~27,500 rounds across 5 courses, ~194 MB raw CSV
    pilot_events_per_round = 150  # Approximate from pilot data
    pilot_raw_mb_per_round = total_raw_mb / total_rounds  # ~0.007 MB per round

    # Parquet compression ratio (typically 5-10x for this type of data)
    parquet_compression_ratio = 7

    # Full scale calculations
    total_projected_rounds = target_courses * target_years * avg_rounds_per_course_year
    total_projected_events = total_projected_rounds * pilot_events_per_round

    raw_storage_gb = (total_projected_rounds * pilot_raw_mb_per_round) / 1024
    parquet_storage_gb = raw_storage_gb / parquet_compression_ratio

    st.markdown("#### Storage projections")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total rounds", f"{total_projected_rounds:,.0f}")
    with col2:
        st.metric("Total events", f"{total_projected_events:,.0f}")
    with col3:
        st.metric("Raw CSV storage", f"{raw_storage_gb:,.0f} GB")
    with col4:
        st.metric("Parquet/Iceberg", f"{parquet_storage_gb:,.0f} GB")

    st.divider()

    # -------------------------------------------------------------------------
    # Processing Time Estimates
    # -------------------------------------------------------------------------
    st.subheader("Processing time estimates")

    # Based on pilot processing (approximate)
    # Pilot: ~27,500 rounds processed in ~5-10 minutes on local Docker
    pilot_rounds_per_minute_local = 3000  # Conservative estimate
    pilot_rounds_per_minute_aws = 10000  # AWS Glue with more resources

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Local (Docker)")
        local_minutes = total_projected_rounds / pilot_rounds_per_minute_local
        local_hours = local_minutes / 60

        st.markdown(
            f"""
        | Metric | Value |
        |--------|-------|
        | Processing rate | ~{pilot_rounds_per_minute_local:,} rounds/min |
        | Total time | **{local_hours:,.0f} hours** ({local_hours/24:.0f} days) |
        | Recommended | Batch by course, parallelize |
        """
        )

        st.warning(
            f"⚠️ Full historical load would take ~{local_hours/24:.0f} days on a single local machine. "
            "Use AWS for production workloads."
        )

    with col2:
        st.markdown("#### AWS (Glue Spark)")
        aws_minutes = total_projected_rounds / pilot_rounds_per_minute_aws
        aws_hours = aws_minutes / 60

        # Parallel processing with multiple Glue jobs
        parallel_jobs = st.slider("Parallel Glue jobs", 1, 50, 10)
        aws_hours_parallel = aws_hours / parallel_jobs

        st.markdown(
            f"""
        | Metric | Value |
        |--------|-------|
        | Processing rate | ~{pilot_rounds_per_minute_aws:,} rounds/min/job |
        | Sequential time | {aws_hours:,.0f} hours |
        | With {parallel_jobs} parallel jobs | **{aws_hours_parallel:,.1f} hours** |
        """
        )

        st.success(
            f"✅ With {parallel_jobs} parallel Glue jobs, full load completes in ~{aws_hours_parallel:.1f} hours"
        )

    st.divider()

    # -------------------------------------------------------------------------
    # AWS Cost Estimation
    # -------------------------------------------------------------------------
    st.subheader("AWS cost estimation")
    st.markdown(
        """
    Estimated costs for running the lakehouse on AWS. Adjust parameters to see how costs scale.
    """
    )

    # Cost inputs
    st.markdown("#### Cost parameters")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Storage (S3)**")
        s3_price_per_gb = st.number_input(
            "S3 Standard $/GB/month", value=0.023, format="%.3f"
        )
        s3_glacier_price = st.number_input(
            "S3 Glacier $/GB/month", value=0.004, format="%.3f"
        )

    with col2:
        st.markdown("**Compute (Glue)**")
        glue_dpu_price = st.number_input("Glue DPU $/hour", value=0.44, format="%.2f")
        glue_dpus_per_job = st.number_input(
            "DPUs per job", value=10, min_value=2, max_value=100
        )

    with col3:
        st.markdown("**Query (Athena)**")
        athena_price_per_tb = st.number_input(
            "Athena $/TB scanned", value=5.00, format="%.2f"
        )
        monthly_query_tb = st.number_input(
            "Est. monthly query TB", value=1.0, format="%.1f"
        )

    # Calculate costs
    st.markdown("#### Monthly cost breakdown")

    # Storage costs
    bronze_storage_gb = raw_storage_gb * 0.1  # Keep 10% in Bronze for replay
    silver_storage_gb = parquet_storage_gb
    gold_storage_gb = parquet_storage_gb * 0.1  # Gold is aggregated, much smaller

    total_hot_storage_gb = silver_storage_gb + gold_storage_gb
    total_cold_storage_gb = bronze_storage_gb

    storage_hot_cost = total_hot_storage_gb * s3_price_per_gb
    storage_cold_cost = total_cold_storage_gb * s3_glacier_price
    total_storage_cost = storage_hot_cost + storage_cold_cost

    # Compute costs (initial load)
    initial_load_hours = aws_hours_parallel
    initial_load_dpu_hours = initial_load_hours * glue_dpus_per_job * parallel_jobs
    initial_load_cost = initial_load_dpu_hours * glue_dpu_price

    # Ongoing compute (daily incremental)
    daily_rounds = (target_courses * avg_rounds_per_course_year) / 365
    daily_process_minutes = daily_rounds / pilot_rounds_per_minute_aws
    daily_dpu_hours = (daily_process_minutes / 60) * glue_dpus_per_job
    monthly_compute_cost = daily_dpu_hours * 30 * glue_dpu_price

    # Query costs
    monthly_query_cost = monthly_query_tb * athena_price_per_tb

    # Total monthly
    total_monthly = total_storage_cost + monthly_compute_cost + monthly_query_cost

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### One-time costs (initial load)")
        st.markdown(
            f"""
        | Item | Cost |
        |------|------|
        | Glue DPU hours | {initial_load_dpu_hours:,.0f} |
        | **Initial load cost** | **${initial_load_cost:,.2f}** |
        """
        )

    with col2:
        st.markdown("##### Monthly recurring costs")
        st.markdown(
            f"""
        | Item | Cost |
        |------|------|
        | S3 Hot storage ({total_hot_storage_gb:,.0f} GB) | ${storage_hot_cost:,.2f} |
        | S3 Cold storage ({total_cold_storage_gb:,.0f} GB) | ${storage_cold_cost:,.2f} |
        | Glue compute (daily ETL) | ${monthly_compute_cost:,.2f} |
        | Athena queries | ${monthly_query_cost:,.2f} |
        | **Total monthly** | **${total_monthly:,.2f}** |
        """
        )

    # Annual summary
    st.markdown("#### Annual cost summary")
    annual_cost = (total_monthly * 12) + initial_load_cost

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Initial load", f"${initial_load_cost:,.0f}")
    with col2:
        st.metric("Monthly recurring", f"${total_monthly:,.0f}")
    with col3:
        st.metric("First year total", f"${annual_cost:,.0f}")

    st.info(
        f"💡 **Cost per course per year:** ${annual_cost / target_courses:,.2f} "
        f"(${annual_cost / target_courses / 12:.2f}/month)"
    )

    st.divider()

    # -------------------------------------------------------------------------
    # AWS Architecture Recommendations
    # -------------------------------------------------------------------------
    st.subheader("AWS architecture recommendations")

    st.markdown(
        """
    ### Recommended AWS Stack

    | Component | Service | Configuration |
    |-----------|---------|---------------|
    | **Storage** | S3 | Standard for Silver/Gold, Glacier for Bronze archive |
    | **Catalog** | Glue Data Catalog | Iceberg table format |
    | **ETL** | AWS Glue | Spark 3.x with Iceberg, 10 DPU per job |
    | **Query** | Athena | Iceberg connector, workgroup with query limits |
    | **Orchestration** | MWAA (Managed Airflow) | Or Step Functions for simpler workflows |
    | **Dashboard** | QuickSight or Streamlit on ECS | Connect to Athena |

    ### Compute sizing guide

    | Workload | Glue DPUs | Est. cost/run |
    |----------|-----------|---------------|
    | Single course daily | 2-4 DPU | $0.50-1.00 |
    | Batch (50 courses) | 10 DPU | $5-10 |
    | Full historical load | 10 DPU × 10 parallel | $500-1000 |

    ### Cost optimization tips

    1. **Use Iceberg partitioning** - Partition by `course_id` and `event_date` for efficient pruning
    2. **Compress with Parquet** - 5-10x compression vs raw CSV
    3. **Archive Bronze to Glacier** - After 30 days, move raw files to Glacier
    4. **Use Athena workgroups** - Set query limits to prevent runaway costs
    5. **Schedule off-peak** - Run batch jobs during off-peak hours for spot pricing
    """
    )

    st.divider()

    # -------------------------------------------------------------------------
    # Local vs AWS Comparison
    # -------------------------------------------------------------------------
    st.subheader("Local vs AWS comparison")

    comparison_data = [
        {
            "Aspect": "Initial setup cost",
            "Local (Docker)": "$0 (existing hardware)",
            "AWS": f"~${initial_load_cost:,.0f} (one-time)",
        },
        {
            "Aspect": "Monthly cost",
            "Local (Docker)": "~$50-100 (electricity, wear)",
            "AWS": f"~${total_monthly:,.0f}",
        },
        {
            "Aspect": "Full load time",
            "Local (Docker)": f"~{local_hours/24:.0f} days",
            "AWS": f"~{aws_hours_parallel:.0f} hours",
        },
        {
            "Aspect": "Scalability",
            "Local (Docker)": "Limited by hardware",
            "AWS": "Virtually unlimited",
        },
        {
            "Aspect": "Maintenance",
            "Local (Docker)": "Self-managed",
            "AWS": "Managed services",
        },
        {
            "Aspect": "Availability",
            "Local (Docker)": "Single point of failure",
            "AWS": "99.9%+ SLA",
        },
        {
            "Aspect": "Best for",
            "Local (Docker)": "Development, testing, small pilots",
            "AWS": "Production, full historical load",
        },
    ]

    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(comparison_df, use_container_width=True, hide_index=True)


# =============================================================================
# PAGE: DATA MODEL
# =============================================================================


def render_data_model():
    """Data Model page - shows the ERD and schema documentation."""
    st.header("Data model")
    st.markdown(
        """
    The Tagmarshal lakehouse uses a **star schema** design with a central fact table
    containing GPS telemetry events and dimension tables for course metadata.
    """
    )

    st.divider()

    # -------------------------------------------------------------------------
    # Interactive ERD
    # -------------------------------------------------------------------------
    st.subheader("Entity relationship diagram")
    st.markdown(
        """
    The diagram below shows the relationships between tables in the Silver layer.
    Click **"Open in dbdiagram.io"** to explore interactively.
    """
    )

    # Read the DBML file
    import urllib.parse
    from pathlib import Path

    dbml_path = Path(__file__).parent.parent / "pipeline" / "silver" / "erd.dbml"
    try:
        dbml_content = dbml_path.read_text()
    except FileNotFoundError:
        dbml_content = "// ERD file not found"

    # Create a link to dbdiagram.io with the DBML content
    dbml_encoded = urllib.parse.quote(dbml_content)
    dbdiagram_url = f"https://dbdiagram.io/d?definition={dbml_encoded}"

    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            f'<a href="{dbdiagram_url}" target="_blank" style="text-decoration:none;">'
            f'<button style="background-color:#4A90D9;color:white;padding:10px 20px;border:none;border-radius:5px;cursor:pointer;font-size:14px;">'
            f"🔗 Open in dbdiagram.io</button></a>",
            unsafe_allow_html=True,
        )
    with col2:
        with st.expander("View DBML source"):
            st.code(dbml_content, language="sql")

    st.divider()

    # -------------------------------------------------------------------------
    # Schema Overview
    # -------------------------------------------------------------------------
    st.subheader("Schema overview")

    # Fact table
    st.markdown("### `fact_telemetry_event` (Fact Table)")
    st.markdown(
        """
    **Grain:** One row per GPS fix per round.  
    **Partitioned by:** `course_id`, `event_date`  
    **Primary key:** `(round_id, location_index)`
    """
    )

    fact_schema = [
        ("round_id", "VARCHAR", "NO", "Unique round identifier (MongoDB _id)"),
        ("course_id", "VARCHAR", "NO", "Course identifier"),
        ("ingest_date", "DATE", "NO", "Date data was ingested"),
        ("location_index", "INT", "NO", "Position in locations array (0-based)"),
        ("fix_timestamp", "TIMESTAMP", "YES", "GPS fix timestamp"),
        ("latitude", "DOUBLE", "YES", "GPS latitude"),
        ("longitude", "DOUBLE", "YES", "GPS longitude"),
        ("hole_number", "INT", "YES", "Current hole (1-9/18/27)"),
        ("section_number", "INT", "YES", "Global section number"),
        ("nine_number", "INT", "YES", "Which nine (1, 2, or 3)"),
        ("pace", "DOUBLE", "YES", "Pace value in minutes"),
        ("pace_gap", "DOUBLE", "YES", "Gap to expected pace"),
        ("battery_percentage", "DOUBLE", "YES", "Device battery (0-100)"),
        ("device", "VARCHAR", "YES", "Device identifier"),
        ("is_location_padding", "BOOLEAN", "NO", "True for placeholder rows"),
        ("is_nine_hole", "BOOLEAN", "YES", "True for 9-hole rounds"),
        ("is_complete", "BOOLEAN", "YES", "True if round completed"),
        ("round_duration_minutes", "DOUBLE", "YES", "Round duration in minutes"),
        ("event_date", "DATE", "YES", "Date from fix_timestamp"),
    ]

    fact_df = pd.DataFrame(
        fact_schema, columns=["Column", "Type", "Nullable", "Description"]
    )
    st.dataframe(fact_df, use_container_width=True, hide_index=True)

    with st.expander("View all columns (50+)"):
        all_columns = [
            "round_id",
            "course_id",
            "ingest_date",
            "location_index",
            "fix_timestamp",
            "is_timestamp_missing",
            "latitude",
            "longitude",
            "geometry_wkt",
            "hole_number",
            "section_number",
            "hole_section",
            "nine_number",
            "pace",
            "pace_gap",
            "positional_gap",
            "device",
            "battery_percentage",
            "is_cache",
            "is_projected",
            "is_problem",
            "is_location_padding",
            "round_start_time",
            "round_end_time",
            "round_duration_minutes",
            "start_hole",
            "start_section",
            "end_section",
            "is_nine_hole",
            "current_nine",
            "goal_time",
            "goal_name",
            "goal_time_fraction",
            "is_complete",
            "is_incomplete",
            "is_secondary",
            "is_auto_assigned",
            "first_fix",
            "last_fix",
            "last_section_start",
            "current_section",
            "current_hole",
            "current_hole_section",
            "event_date",
            "event_year",
            "event_month",
            "event_day",
            "event_weekday",
        ]
        st.code("\n".join(all_columns), language="text")

    st.divider()

    # Dimension tables
    st.markdown("### Dimension Tables")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### `dim_facility_topology`")
        st.markdown("Maps courses to their nine structure and section ranges.")
        topology_schema = [
            ("facility_id", "VARCHAR", "Course identifier"),
            ("unit_id", "INT", "Nine identifier (1, 2, 3)"),
            ("unit_name", "VARCHAR", "Name (Front Nine, etc.)"),
            ("nine_number", "INT", "Which nine"),
            ("section_start", "INT", "First section in nine"),
            ("section_end", "INT", "Last section in nine"),
        ]
        topo_df = pd.DataFrame(
            topology_schema, columns=["Column", "Type", "Description"]
        )
        st.dataframe(topo_df, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("#### `dim_course_profile`")
        st.markdown("Human-entered course metadata and configuration.")
        profile_schema = [
            ("course_id", "VARCHAR", "Course identifier"),
            ("course_type", "VARCHAR", "Type (9-hole loop, 18-hole, etc.)"),
            ("is_loop_course", "BOOLEAN", "Supports 18-hole via loop"),
            ("volume_profile", "VARCHAR", "Volume (high/medium/low)"),
            ("notes", "VARCHAR", "Human notes"),
        ]
        profile_df = pd.DataFrame(
            profile_schema, columns=["Column", "Type", "Description"]
        )
        st.dataframe(profile_df, use_container_width=True, hide_index=True)

    st.divider()

    # -------------------------------------------------------------------------
    # Live Data Preview
    # -------------------------------------------------------------------------
    st.subheader("Live data preview")
    st.markdown("Sample data from each table in the lakehouse.")

    tab1, tab2, tab3 = st.tabs(
        ["fact_telemetry_event", "dim_facility_topology", "dim_course_profile"]
    )

    with tab1:
        sample_q = run_query(
            name="FACT_SAMPLE",
            sql="""
            SELECT 
                round_id, course_id, location_index, fix_timestamp,
                hole_number, section_number, nine_number, pace,
                is_location_padding, is_nine_hole
            FROM iceberg.silver.fact_telemetry_event
            WHERE is_location_padding = FALSE
            LIMIT 10
            """,
        )
        if sample_q.ok and sample_q.df is not None:
            display_df(sample_q.df)
        render_query_debug(sample_q)

    with tab2:
        topo_q = run_query(name="TOPOLOGY_SAMPLE", sql=queries.TOPOLOGY)
        if topo_q.ok and topo_q.df is not None:
            display_df(topo_q.df)
        render_query_debug(topo_q)

    with tab3:
        profile_q = run_query(name="PROFILE_SAMPLE", sql=queries.COURSE_PROFILE)
        if profile_q.ok and profile_q.df is not None:
            display_df(profile_q.df)
        render_query_debug(profile_q)

    st.divider()

    # -------------------------------------------------------------------------
    # Data Lineage
    # -------------------------------------------------------------------------
    st.subheader("Data lineage")
    st.markdown(
        """
    ```
    ┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
    │   BRONZE LAYER      │     │   SILVER LAYER      │     │    GOLD LAYER       │
    │   (Landing Zone)    │────▶│   (Iceberg Tables)  │────▶│   (dbt Models)      │
    └─────────────────────┘     └─────────────────────┘     └─────────────────────┘
           │                           │                           │
           │  CSV/JSON exports         │  fact_telemetry_event     │  Aggregated views
           │  from MongoDB             │  dim_facility_topology    │  Quality scores
           │                           │  dim_course_profile       │  Pace summaries
    ```
    
    **Pipeline:**
    1. **Bronze:** Raw CSV/JSON files ingested from Tagmarshal MongoDB exports
    2. **Silver:** Cleaned, exploded (one row per GPS fix), and enriched with derived columns
    3. **Gold:** Aggregated models built with dbt for reporting and analytics
    """
    )

    st.divider()

    # -------------------------------------------------------------------------
    # Key Concepts
    # -------------------------------------------------------------------------
    st.subheader("Key concepts")

    concepts = {
        "Round": "A single play session by a group, identified by `round_id`. Contains multiple GPS fixes.",
        "Location/Fix": "A single GPS reading during a round, identified by `(round_id, location_index)`.",
        "Section": "A subdivision of the course (typically 2-3 per hole). Global numbering across all holes.",
        "Nine": "A group of 9 holes. 18-hole courses have 2 nines, 27-hole courses have 3.",
        "Loop Course": "A 9-hole course that supports 18-hole rounds by playing the same 9 holes twice.",
        "Padding": "Placeholder rows in CSV exports where no telemetry was collected. Marked with `is_location_padding=true`.",
        "Pace": "Time relative to goal. Negative = ahead of pace, Positive = behind pace.",
    }

    for term, definition in concepts.items():
        st.markdown(f"**{term}:** {definition}")


# =============================================================================
# PAGE: PROJECT CONTEXT
# =============================================================================


def render_project_context():
    st.header("Project context")
    st.markdown(
        """
    This dashboard is part of the Tagmarshal data strategy project, which aims to
    build a scalable lakehouse on AWS for round data.
    """
    )

    st.divider()

    st.subheader("Current phase: foundation and local build")

    st.markdown(
        """
    **What we're doing:**
    - Working with historical CSV exports from 5 pilot courses
    - Building a local 'lakehouse in a box' to validate the data model
    - Surfacing data quality metrics to guide decisions
    
    **What we've learned:**
    - The data contains padding records that need to be handled
    - Course topology varies significantly (9, 18, and 27-hole configurations)
    - Data completeness varies by field and by course
    """
    )

    st.divider()

    st.subheader("Architecture: medallion pattern")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Bronze (raw)**")
        st.markdown(
            """
        Landing zone for incoming files.
        - Original CSV/JSON format
        - No transformations
        - Full history preserved
        """
        )

    with col2:
        st.markdown("**Silver (cleaned)**")
        st.markdown(
            """
        Standardised, validated data.
        - Schema enforcement
        - Coordinate validation
        - Timestamps normalised
        """
        )

    with col3:
        st.markdown("**Gold (analytics)**")
        st.markdown(
            """
        Business-ready tables.
        - Aggregated metrics
        - Pace of play analysis
        - Round-level facts
        """
        )

    st.divider()

    st.subheader("Pilot courses")

    pilots = [
        ("Pinehurst 4", "18-hole", "North Carolina"),
        ("Erin Hills", "18-hole", "Wisconsin"),
        ("Indian Creek", "18-hole", "Florida"),
        ("Bradshaw Farm GC", "27-hole", "Georgia"),
        ("American Falls", "9-hole", "Idaho"),
    ]

    for name, type_, location in pilots:
        st.markdown(f"- **{name}** — {type_} ({location})")

    st.divider()

    st.subheader("Next steps")

    st.markdown(
        """
    1. **AWS Deployment** — Move the local pipeline into AWS
    2. **Expand to 20-50 courses** — Validate at scale
    3. **Pilot Dashboard** — Build operational insights
    4. **Full Fleet Rollout** — 650+ courses globally
    """
    )


# =============================================================================
# MAIN ROUTER
# =============================================================================

if page == "Data quality":
    render_data_quality()
elif page == "Course topology":
    render_topology()
elif page == "Round insights":
    render_round_insights()
elif page == "Pace analysis":
    render_pace_analysis()
elif page == "Global metrics":
    render_global_metrics()
elif page == "Infrastructure":
    render_infrastructure()
elif page == "Data model":
    render_data_model()
elif page == "Project context":
    render_project_context()
