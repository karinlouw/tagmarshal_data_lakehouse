"""Course topology page.

Demonstrates the dynamic topology data model and how we handle complex course
layouts: 27-hole facilities, 9-hole loops, and shotgun starts.
"""

import streamlit as st
import pandas as pd
import plotly.express as px

from utils.database import execute_query
from utils import queries
from utils.colors import (
    COLOR_SEQUENCE,
    COLOR_GOOD,
    COLOR_CRITICAL,
)
from utils.dbt_provenance import render_dbt_models_section


def render():
    """Render the course topology page."""

    st.title("Course topology model")
    st.caption("Dynamic mapping of telemetry data to physical course units")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Topology table explorer
    # -------------------------------------------------------------------------
    st.header("Topology configuration")

    try:
        topology_df = execute_query(queries.FACILITY_TOPOLOGY)

        if len(topology_df) > 0:
            st.dataframe(
                topology_df.style.format(
                    {
                        "section_start": "{:.0f}",
                        "section_end": "{:.0f}",
                    }
                ),
                use_container_width=True,
            )
        else:
            st.info("No topology data found. Run `just topology-refresh` to generate.")

    except Exception as e:
        st.warning(f"Could not load topology data: {e}")
        topology_df = pd.DataFrame()

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Scenario 1: 27-hole facility (Bradshaw Farm)
    # -------------------------------------------------------------------------
    st.header("27-hole facility: Bradshaw Farm")

    col1, col2 = st.columns(2)

    with col1:
        bradshaw_topo = topology_df[topology_df["facility_id"] == "bradshawfarmgc"]
        if len(bradshaw_topo) > 0:
            # Visual section range chart
            bradshaw_topo = bradshaw_topo.sort_values("nine_number")
            display_topo = bradshaw_topo[
                ["unit_name", "nine_number", "section_start", "section_end"]
            ].copy()
            display_topo["section_range"] = display_topo.apply(
                lambda row: f"{int(row['section_start'])}-{int(row['section_end'])}",
                axis=1,
            )
            display_topo = display_topo.rename(
                columns={
                    "unit_name": "Unit",
                    "nine_number": "Nine",
                    "section_range": "Sections",
                }
            )[["Unit", "Nine", "Sections"]]
            st.dataframe(display_topo, use_container_width=True, hide_index=True)
        else:
            st.info("Bradshaw topology not yet configured.")

    with col2:

        try:
            bradshaw_df = execute_query(queries.TOPOLOGY_VALIDATION_BRADSHAW)

            if len(bradshaw_df) > 0:
                fig = px.bar(
                    bradshaw_df,
                    x="nine_number",
                    y="fixes",
                    color="nine_number",
                    title="Telemetry events by unit (Bradshaw)",
                    labels={"fixes": "Events", "nine_number": "Unit"},
                    color_discrete_sequence=COLOR_SEQUENCE,
                )
                fig.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No Bradshaw data available.")

        except Exception as e:
            st.warning(f"Could not load Bradshaw validation data: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Scenario 2: 9-hole loop (American Falls)
    # -------------------------------------------------------------------------
    st.header("9-hole loop: American Falls")
    st.caption("Same 9 holes played twice - measuring fatigue factor")

    try:
        fatigue_df = execute_query(queries.LOOP_FATIGUE_AMERICAN_FALLS)

        if len(fatigue_df) > 0:
            col1, col2 = st.columns(2)

            with col1:
                pivot_df = fatigue_df.pivot_table(
                    index="hole_number",
                    columns="nine_number",
                    values="avg_pace_seconds",
                ).reset_index()

                if 1 in pivot_df.columns and 2 in pivot_df.columns:
                    pivot_df["difference"] = pivot_df[2] - pivot_df[1]
                    pivot_df = pivot_df.rename(columns={1: "Loop 1", 2: "Loop 2"})

                    st.dataframe(
                        pivot_df.style.format(
                            {
                                "Loop 1": "{:.2f}s",
                                "Loop 2": "{:.2f}s",
                                "difference": "{:+.2f}s",
                            }
                        ),
                        use_container_width=True,
                    )

            with col2:
                fig = px.bar(
                    fatigue_df,
                    x="hole_number",
                    y="avg_pace_seconds",
                    color="nine_number",
                    barmode="group",
                    title="Pace comparison: Loop 1 vs Loop 2",
                    labels={
                        "avg_pace_seconds": "Avg pace (seconds)",
                        "hole_number": "Hole",
                        "nine_number": "Loop",
                    },
                    color_discrete_map={1: COLOR_GOOD, 2: COLOR_CRITICAL},
                )
                fig.update_layout(
                    height=350,
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

            if (
                len(fatigue_df) > 0
                and 1 in fatigue_df["nine_number"].values
                and 2 in fatigue_df["nine_number"].values
            ):
                loop1_avg = fatigue_df[fatigue_df["nine_number"] == 1][
                    "avg_pace_seconds"
                ].mean()
                loop2_avg = fatigue_df[fatigue_df["nine_number"] == 2][
                    "avg_pace_seconds"
                ].mean()
                diff = loop2_avg - loop1_avg

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Loop 1 avg pace", f"{loop1_avg:.0f}s")
                with col2:
                    st.metric("Loop 2 avg pace", f"{loop2_avg:.0f}s")
                with col3:
                    st.metric(
                        "Difference", f"{diff:+.0f}s", delta=f"{diff:+.0f}s slower"
                    )
        else:
            st.info("No American Falls loop data available.")

    except Exception as e:
        st.warning(f"Could not load fatigue analysis data: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Scenario 3: Shotgun starts (Indian Creek)
    # -------------------------------------------------------------------------
    st.header("Shotgun starts: Indian Creek")
    st.caption("Rounds starting from multiple holes simultaneously")

    try:
        shotgun_df = execute_query(queries.SHOTGUN_START_DISTRIBUTION)

        if len(shotgun_df) > 0:
            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    shotgun_df.sort_values("rounds", ascending=True),
                    x="rounds",
                    y="start_hole",
                    orientation="h",
                    title="Rounds by start hole",
                    labels={"rounds": "Number of rounds", "start_hole": "Start hole"},
                    color="rounds",
                    color_continuous_scale="Blues",
                )
                fig.update_layout(
                    height=350,
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.pie(
                    shotgun_df,
                    values="rounds",
                    names="start_hole",
                    title="Distribution by start hole",
                    color_discrete_sequence=COLOR_SEQUENCE,
                )
                fig.update_layout(
                    height=350,
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

            unique_starts = len(shotgun_df)
            if unique_starts > 1:
                st.metric("Unique start holes", unique_starts)
        else:
            st.info("No Indian Creek shotgun start data available.")

    except Exception as e:
        st.warning(f"Could not load shotgun start data: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Technical implementation
    # -------------------------------------------------------------------------
    with st.expander("Technical details"):
        st.markdown(
            """
        **Topology enrichment:** Silver ETL joins telemetry with topology table to assign `nine_number`.
        
        **Outlier resistance:** Uses frequency thresholds (min 25 fixes per section) to avoid GPS artifacts.
        """
        )

    st.markdown("---")
    render_dbt_models_section(["fact_round_hole_performance.sql"])
