"""Course topology page.

Demonstrates the dynamic topology data model and how we handle complex course
layouts: 27-hole facilities, 9-hole loops, and shotgun starts.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.database import execute_query
from utils import queries
from utils.colors import (
    COLOR_SEQUENCE,
    COLOR_GOOD,
    COLOR_CRITICAL,
)


def render():
    """Render the course topology page."""

    st.title("Course topology model")
    st.markdown(
        """
    This page demonstrates how we handle the **complex and dynamic nature** of 
    golf course layouts. The challenge: "Hole 1" is ambiguous without context.
    
    Our solution is a **topology-first approach** that dynamically maps telemetry 
    data to physical course units.
    """
    )

    # -------------------------------------------------------------------------
    # The problem
    # -------------------------------------------------------------------------
    st.header("The challenge")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
        **Why is this complex?**
        
        Golf courses vary significantly in layout:
        
        - **27-hole facilities** have 3 sets of 9 holes (e.g., Red, White, Blue)
        - **9-hole loop courses** replay the same 9 holes for an 18-hole round
        - **Shotgun starts** begin groups on different holes simultaneously
        
        Without proper handling, "Hole 5" could mean:
        - Red Course Hole 5
        - White Course Hole 5
        - Hole 5 on the first loop
        - Hole 5 on the second loop
        """
        )

    with col2:
        st.markdown(
            """
        **Our solution: Dynamic topology mapping**
        
        We automatically infer the course layout from the telemetry data:
        
        1. **Scan section ranges** from observed GPS fixes
        2. **Detect unit boundaries** where hole numbers reset
        3. **Generate topology configuration** mapping sections to units
        4. **Enrich telemetry** with `nine_number` for accurate analysis
        
        This approach is:
        - ✅ Data-driven (no hardcoded assumptions)
        - ✅ Outlier-resistant (uses frequency thresholds)
        - ✅ Flexible (works for any course layout)
        """
        )

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Topology table explorer
    # -------------------------------------------------------------------------
    st.header("Topology configuration")
    st.markdown(
        """
    The `dim_facility_topology` table maps section number ranges to physical course units.
    This is automatically generated from the telemetry data.
    """
    )

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

        with st.expander("SQL query"):
            st.code(queries.FACILITY_TOPOLOGY, language="sql")

    except Exception as e:
        st.warning(f"Could not load topology data: {e}")
        topology_df = pd.DataFrame()

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Scenario 1: 27-hole facility (Bradshaw Farm)
    # -------------------------------------------------------------------------
    st.header("Scenario 1: 27-hole facility")
    st.markdown(
        """
    **Bradshaw Farm GC** is a 27-hole facility with three distinct sets of 9 holes.
    
    The topology model maps section numbers to units:
    - **Unit 1**: Sections 1-27 (holes 1-9)
    - **Unit 2**: Sections 28-54 (holes 10-18 or 1-9 depending on layout)
    - **Unit 3**: Sections 55-81 (holes 19-27 or 1-9 depending on layout)
    """
    )

    col1, col2 = st.columns(2)

    with col1:
        # Visual diagram
        st.markdown("**Section-to-unit mapping:**")

        bradshaw_topo = topology_df[topology_df["facility_id"] == "bradshawfarmgc"]
        if len(bradshaw_topo) > 0:
            for _, row in bradshaw_topo.iterrows():
                st.markdown(
                    f"""
                🏌️ **{row['unit_name']}** (Nine {row['nine_number']})
                - Sections: {row['section_start']:.0f} → {row['section_end']:.0f}
                """
                )
        else:
            st.info("Bradshaw topology not yet configured.")

    with col2:
        # Validation data
        st.markdown("**Validation: Events by unit**")

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

    with st.expander("SQL query"):
        st.code(queries.TOPOLOGY_VALIDATION_BRADSHAW, language="sql")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Scenario 2: 9-hole loop (American Falls)
    # -------------------------------------------------------------------------
    st.header("Scenario 2: 9-hole loop course")
    st.markdown(
        """
    **American Falls** is a 9-hole course where players complete two loops for an 
    18-hole round.
    
    The challenge: How do we differentiate "Hole 5 (first loop)" from "Hole 5 (second loop)"?
    
    **Solution:** We use `nine_number` to distinguish loops:
    - `nine_number = 1`: First 9 holes (fresh)
    - `nine_number = 2`: Second 9 holes (fatigued/congested)
    """
    )

    st.subheader("Fatigue factor analysis")
    st.markdown(
        """
    Playing the same physical hole twice allows us to measure the **fatigue factor**:
    Do players slow down on the second loop?
    """
    )

    try:
        fatigue_df = execute_query(queries.LOOP_FATIGUE_AMERICAN_FALLS)

        if len(fatigue_df) > 0:
            col1, col2 = st.columns(2)

            with col1:
                # Pivot for comparison
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
                # Bar chart comparison
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

            # Key insight
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

                st.success(
                    f"""
                **Key insight:** Players are on average **{diff:+.0f} seconds** slower on the second loop.
                This demonstrates the value of differentiating loops in the data model.
                """
                )
        else:
            st.info("No American Falls loop data available.")

    except Exception as e:
        st.warning(f"Could not load fatigue analysis data: {e}")

    with st.expander("SQL query"):
        st.code(queries.LOOP_FATIGUE_AMERICAN_FALLS, language="sql")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Scenario 3: Shotgun starts (Indian Creek)
    # -------------------------------------------------------------------------
    st.header("Scenario 3: Shotgun starts")
    st.markdown(
        """
    **Indian Creek** is an exclusive facility where rounds often start from 
    different holes (shotgun start pattern). This is common for tournaments 
    and member events.
    
    **Challenge:** Traditional sequential processing assumes everyone starts at Hole 1.
    
    **Solution:** We track the actual `start_hole` and process rounds non-sequentially.
    """
    )

    try:
        shotgun_df = execute_query(queries.SHOTGUN_START_DISTRIBUTION)

        if len(shotgun_df) > 0:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Start hole distribution:**")
                st.dataframe(
                    shotgun_df.style.format({"rounds": "{:,.0f}"}),
                    use_container_width=True,
                )

            with col2:
                fig = px.pie(
                    shotgun_df,
                    values="rounds",
                    names="start_hole",
                    title="Rounds by start hole (Indian Creek)",
                    color_discrete_sequence=COLOR_SEQUENCE,
                )
                fig.update_layout(
                    height=350,
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

            # Key insight
            unique_starts = len(shotgun_df)
            if unique_starts > 1:
                st.info(
                    f"""
                **Key insight:** Indian Creek has **{unique_starts} different start holes** observed,
                confirming shotgun start patterns. Our topology model handles this by tracking 
                `start_hole` at the round level and processing holes independently of start order.
                """
                )
        else:
            st.info("No Indian Creek shotgun start data available.")

    except Exception as e:
        st.warning(f"Could not load shotgun start data: {e}")

    with st.expander("SQL query"):
        st.code(queries.SHOTGUN_START_DISTRIBUTION, language="sql")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Technical implementation
    # -------------------------------------------------------------------------
    st.header("Technical implementation")

    with st.expander("How topology enrichment works"):
        st.markdown(
            """
        The Silver ETL joins telemetry events with the topology table to assign 
        the correct `nine_number` to each GPS fix:
        
        ```sql
        -- Topology enrichment join
        SELECT 
            t.*,
            topo.nine_number,
            topo.unit_name
        FROM fact_telemetry_event t
        LEFT JOIN dim_facility_topology topo
            ON t.course_id = topo.facility_id
            AND t.section_number >= topo.section_start
            AND t.section_number <= topo.section_end
        ```
        
        **Priority order for `nine_number`:**
        1. Device-reported `current_nine` (if trusted)
        2. Topology join result
        3. Fallback heuristic (section_number / 27)
        """
        )

    with st.expander("Outlier-resistant boundaries"):
        st.markdown(
            """
        Topology inference uses frequency-aware section ranges to avoid GPS artifacts:
        
        - Only sections with at least **25 fixes** are considered reliable
        - A small **pad of 2** is applied to include legitimate edge sections
        - This prevents rare outlier sections from distorting boundaries
        
        Configurable via environment variables:
        - `TM_TOPOLOGY_MIN_FIXES_PER_SECTION` (default: 25)
        - `TM_TOPOLOGY_RELIABLE_RANGE_PAD` (default: 2)
        """
        )
