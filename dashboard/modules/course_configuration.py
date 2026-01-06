"""Course configuration page.

Displays course type analysis, shotgun start patterns, round completion rates,
and complexity scores.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.database import execute_query
from utils import queries
from utils.colors import (
    COLOR_SEQUENCE,
    COLOR_MAP_COMPLETE,
    COLOR_MAP_ROUND_TYPE,
    COLOR_SCALE_QUALITY,
    COLOR_INFO,
)


def render():
    """Render the course configuration page."""

    st.title("Course configuration analysis")
    st.markdown(
        """
    This page analyses the complexity and configuration patterns of each course,
    helping to understand the variety of course setups in the data.
    """
    )

    try:
        config_df = execute_query(queries.COURSE_CONFIGURATION)
    except Exception as e:
        st.error(f"Failed to load course configuration data: {e}")
        return

    # -------------------------------------------------------------------------
    # Course type summary
    # -------------------------------------------------------------------------
    st.header("Course type breakdown")

    col1, col2 = st.columns([1, 2])

    with col1:
        # Course type pie chart
        type_counts = config_df["likely_course_type"].value_counts().reset_index()
        type_counts.columns = ["course_type", "count"]

        fig = px.pie(
            type_counts,
            values="count",
            names="course_type",
            title="Courses by type",
            color_discrete_sequence=COLOR_SEQUENCE,
        )
        fig.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Course type details
        st.markdown("**Course types detected:**")

        for _, row in config_df.iterrows():
            course_type = row["likely_course_type"]
            if course_type == "27-hole":
                icon = "🏌️🏌️🏌️"
                desc = "Three sets of 9 holes"
            elif course_type == "18-hole":
                icon = "🏌️🏌️"
                desc = "Standard 18-hole course"
            else:
                icon = "🏌️"
                desc = "9-hole course (may be played as loop)"

            st.markdown(
                f"""
            **{row['course_id']}**: {icon} {course_type}
            - {row['total_rounds']:,} rounds
            - Max section: {row['max_section_seen']}
            - {desc}
            """
            )

    with st.expander("SQL query"):
        st.code(queries.COURSE_CONFIGURATION, language="sql")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Round completion analysis
    # -------------------------------------------------------------------------
    st.header("Round completion rates")
    st.markdown(
        """
    Percentage of rounds marked as complete vs incomplete. High incomplete rates 
    may indicate tracking issues or abandoned rounds.
    """
    )

    # Stacked bar chart for completion
    completion_data = config_df[["course_id", "pct_complete", "pct_incomplete"]].copy()
    completion_data = completion_data.melt(
        id_vars=["course_id"],
        value_vars=["pct_complete", "pct_incomplete"],
        var_name="status",
        value_name="percentage",
    )
    completion_data["status"] = completion_data["status"].map(
        {"pct_complete": "Complete", "pct_incomplete": "Incomplete"}
    )

    fig = px.bar(
        completion_data,
        x="course_id",
        y="percentage",
        color="status",
        title="Round completion by course",
        labels={"percentage": "Percentage (%)", "course_id": "Course"},
        color_discrete_map=COLOR_MAP_COMPLETE,
        barmode="stack",
    )
    fig.update_layout(
        height=350,
        margin=dict(l=0, r=0, t=40, b=0),
        legend_title_text="Status",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Metrics row
    cols = st.columns(len(config_df))
    for idx, (_, row) in enumerate(config_df.iterrows()):
        with cols[idx]:
            pct = row["pct_complete"] or 0
            st.metric(
                label=row["course_id"],
                value=f"{pct:.2f}%",
                delta="complete",
            )

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Shotgun start analysis
    # -------------------------------------------------------------------------
    st.header("Shotgun start patterns")
    st.markdown(
        """
    Shotgun starts occur when groups begin on different holes simultaneously.
    This is common at exclusive facilities for tournament play.
    """
    )

    col1, col2 = st.columns(2)

    with col1:
        # Shotgun start percentage bar chart
        fig = px.bar(
            config_df.sort_values("pct_shotgun_starts", ascending=False),
            x="course_id",
            y="pct_shotgun_starts",
            title="Shotgun start percentage by course",
            labels={"pct_shotgun_starts": "Shotgun starts (%)", "course_id": "Course"},
            color="pct_shotgun_starts",
            color_continuous_scale=COLOR_SCALE_QUALITY,
        )
        fig.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=40, b=0),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Unique start holes
        fig = px.bar(
            config_df.sort_values("unique_start_holes", ascending=False),
            x="course_id",
            y="unique_start_holes",
            title="Unique start holes observed",
            labels={"unique_start_holes": "Start holes", "course_id": "Course"},
            color="unique_start_holes",
            color_continuous_scale=COLOR_SCALE_QUALITY,
        )
        fig.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=40, b=0),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # 9-hole vs full round breakdown
    # -------------------------------------------------------------------------
    st.header("Round type distribution")
    st.markdown(
        """
    Breakdown of 9-hole rounds vs full 18-hole rounds. 
    Useful for understanding play patterns and course utilisation.
    """
    )

    round_type_data = config_df[
        ["course_id", "pct_nine_hole", "pct_full_rounds"]
    ].copy()
    round_type_data = round_type_data.melt(
        id_vars=["course_id"],
        value_vars=["pct_nine_hole", "pct_full_rounds"],
        var_name="round_type",
        value_name="percentage",
    )
    round_type_data["round_type"] = round_type_data["round_type"].map(
        {"pct_nine_hole": "9-hole", "pct_full_rounds": "Full round"}
    )

    fig = px.bar(
        round_type_data,
        x="course_id",
        y="percentage",
        color="round_type",
        title="9-hole vs full round distribution",
        labels={"percentage": "Percentage (%)", "course_id": "Course"},
        barmode="group",
        color_discrete_map=COLOR_MAP_ROUND_TYPE,
    )
    fig.update_layout(
        height=350,
        margin=dict(l=0, r=0, t=40, b=0),
        legend_title_text="Round type",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Complexity scores
    # -------------------------------------------------------------------------
    st.header("Course complexity scores")
    st.markdown(
        """
    Complexity score combines multiple factors:
    - Course type (27-hole = highest complexity)
    - Number of unique start holes (shotgun patterns)
    - Mix of round types (9-hole vs full)
    - Incomplete round rates
    
    Higher scores indicate more complex tracking requirements.
    """
    )

    fig = px.bar(
        config_df.sort_values("course_complexity_score", ascending=True),
        x="course_complexity_score",
        y="course_id",
        orientation="h",
        title="Course complexity score",
        labels={"course_complexity_score": "Complexity score", "course_id": "Course"},
        color="course_complexity_score",
        color_continuous_scale=COLOR_SCALE_QUALITY,
    )
    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=40, b=0),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Detailed configuration table
    with st.expander("View full configuration data"):
        display_cols = [
            "course_id",
            "total_rounds",
            "likely_course_type",
            "max_section_seen",
            "pct_complete",
            "pct_shotgun_starts",
            "unique_start_holes",
            "avg_locations_per_round",
            "course_complexity_score",
        ]
        st.dataframe(
            config_df[display_cols].style.format(
                {
                    "total_rounds": "{:,.0f}",
                    "pct_complete": "{:.2f}%",
                    "pct_shotgun_starts": "{:.2f}%",
                    "avg_locations_per_round": "{:.0f}",
                    "course_complexity_score": "{:.0f}",
                }
            ),
            use_container_width=True,
        )
