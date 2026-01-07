"""Course configuration page.

Displays course type analysis, shotgun start patterns, and round completion rates.
"""

import streamlit as st
import plotly.express as px

from utils.database import execute_query
from utils import queries
from utils.colors import (
    COLOR_SEQUENCE,
    COLOR_MAP_COMPLETE,
    COLOR_MAP_ROUND_TYPE,
    COLOR_SCALE_QUALITY,
    COLOR_INFO,
)
from utils.dbt_provenance import render_dbt_models_section


def render():
    """Render the course configuration page."""

    st.title("Course configuration analysis")
    st.markdown(
        """
    This page analyses the configuration patterns of each course,
    helping to understand the variety of course setups in the data.
    """
    )

    try:
        config_df = execute_query(queries.COURSE_CONFIGURATION)
        # Always sort courses alphabetically
        config_df = config_df.sort_values("course_id", ascending=True).reset_index(drop=True)
    except Exception as e:
        st.error(f"Failed to load course configuration data: {e}")
        return

    # -------------------------------------------------------------------------
    # Course type summary
    # -------------------------------------------------------------------------
    st.header("Course type breakdown")

    # Chart (40%) + table (60%) layout
    col1, col2 = st.columns([2, 3], gap="large")

    with col1:
        type_counts = config_df["likely_course_type"].value_counts().reset_index()
        type_counts.columns = ["course_type", "count"]

        fig = px.pie(
            type_counts,
            values="count",
            names="course_type",
            title="Courses by type",
            color_discrete_sequence=COLOR_SEQUENCE,
            hole=0.4,  # Makes it a donut chart
        )
        fig.update_traces(
            textposition="inside",
            textinfo="label+percent",
            textfont=dict(color="white", size=12),
            insidetextorientation="auto",
        )
        fig.update_layout(
            height=340,
            margin=dict(l=0, r=0, t=40, b=0),
            showlegend=False,  # Hide legend, show labels on chart
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Course types detected**")

        type_desc_map = {
            "27-hole": "Three sets of 9 holes",
            "18-hole": "Standard 18-hole course",
            "9-hole": "9-hole course (may be played as loop)",
        }

        type_icon_map = {"27-hole": "🏌️🏌️🏌️", "18-hole": "🏌️🏌️", "9-hole": "🏌️"}

        course_type_table = config_df[
            ["course_id", "likely_course_type", "total_rounds", "max_section_seen"]
        ].copy()
        course_type_table["course_type"] = course_type_table["likely_course_type"].map(
            lambda t: f"{type_icon_map.get(t, '')} {t}".strip()
        )
        course_type_table["description"] = course_type_table["likely_course_type"].map(
            lambda t: type_desc_map.get(t, "")
        )

        course_type_table = course_type_table.drop(columns=["likely_course_type"]).rename(
            columns={
                "course_id": "Course",
                "course_type": "Type",
                "total_rounds": "Rounds",
                "max_section_seen": "Max section",
                "description": "Notes",
            }
        )

        course_type_table = course_type_table.sort_values("Course", ascending=True).reset_index(drop=True)

        st.dataframe(
            course_type_table.style.format({"Rounds": "{:,.0f}"}).hide(axis="index"),
            use_container_width=True,
        )

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

    cols = st.columns(len(config_df))
    for idx, (_, row) in enumerate(config_df.iterrows()):
        with cols[idx]:
            pct = row["pct_complete"] or 0
            st.metric(
                label=row["course_id"],
                value=f"{pct:.1f}%",
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

    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        plot_df = config_df.sort_values("pct_shotgun_starts", ascending=True).copy()
        fig = px.bar(
            plot_df,
            y="course_id",
            x="pct_shotgun_starts",
            orientation="h",
            title="Shotgun start rate by course",
            labels={"pct_shotgun_starts": "Shotgun starts (%)", "course_id": ""},
            color="pct_shotgun_starts",
            color_continuous_scale=COLOR_SCALE_QUALITY,
        )
        fig.update_traces(text=plot_df["pct_shotgun_starts"], texttemplate="%{text:.1f}%", textposition="outside")
        fig.update_layout(
            template="plotly_white",
            height=340,
            margin=dict(l=0, r=0, t=50, b=0),
            showlegend=False,
        )
        fig.update_xaxes(range=[0, max(5.0, float(plot_df["pct_shotgun_starts"].max() or 0) * 1.2)], ticksuffix="%")
        fig.update_yaxes(categoryorder="array", categoryarray=plot_df["course_id"].tolist())
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        plot_df = config_df.sort_values("unique_start_holes", ascending=True).copy()
        fig = px.bar(
            plot_df,
            y="course_id",
            x="unique_start_holes",
            orientation="h",
            title="Unique start holes observed",
            labels={"unique_start_holes": "Start holes", "course_id": ""},
            color="unique_start_holes",
            color_continuous_scale=COLOR_SCALE_QUALITY,
        )
        fig.update_traces(text=plot_df["unique_start_holes"], texttemplate="%{text:.0f}", textposition="outside")
        fig.update_layout(
            template="plotly_white",
            height=340,
            margin=dict(l=0, r=0, t=50, b=0),
            showlegend=False,
        )
        fig.update_xaxes(range=[0, max(3.0, float(plot_df["unique_start_holes"].max() or 0) * 1.2)])
        fig.update_yaxes(categoryorder="array", categoryarray=plot_df["course_id"].tolist())
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

    st.markdown("---")
    render_dbt_models_section(["course_configuration_analysis.sql"])
