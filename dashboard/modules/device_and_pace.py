"""Device and pace analysis page.

Displays device health metrics (battery, signal quality) and bottleneck
analysis with an interactive map showing hole-by-hole pace issues.
"""

import streamlit as st
import plotly.express as px

from utils.database import execute_query
from utils import queries
from utils.dbt_provenance import render_dbt_models_section
from utils.colors import (
    COLOR_SCALE_QUALITY,
    COLOR_SCALE_REVERSE,
    COLOR_WARNING,
    COLOR_CRITICAL,
    COLOR_INFO,
)


def render():
    """Render the device and pace analysis page."""

    st.title("Device and pace analysis")
    st.markdown(
        """
    This page combines device health monitoring with pace-of-play analysis.
    Use this to identify hardware issues and bottleneck locations on the course.
    """
    )

    # -------------------------------------------------------------------------
    # Device health section
    # -------------------------------------------------------------------------
    st.header("Device health")
    st.markdown(
        """
    Tracking device health is essential for data quality. Low battery and 
    signal issues can cause data gaps and inaccurate readings.
    """
    )

    col1, col2 = st.columns(2)

    # Battery health
    with col1:
        st.subheader("Battery health")

        try:
            battery_df = execute_query(queries.BATTERY_HEALTH)

            if len(battery_df) > 0:
                fig = px.bar(
                    battery_df.sort_values("pct_low_battery", ascending=False),
                    x="course_id",
                    y="pct_low_battery",
                    title="Low battery events by course (< 20%)",
                    labels={
                        "pct_low_battery": "Low battery (%)",
                        "course_id": "Course",
                    },
                    color="pct_low_battery",
                    color_continuous_scale=COLOR_SCALE_QUALITY,
                )
                fig.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

                # Metrics
                cols = st.columns(len(battery_df))
                for idx, (_, row) in enumerate(battery_df.iterrows()):
                    with cols[idx]:
                        pct = row["pct_low_battery"] or 0
                        st.metric(
                            label=row["course_id"],
                            value=f"{pct:.2f}%",
                            delta="low battery",
                            delta_color="inverse",
                        )

            else:
                st.info("No battery data available.")

        except Exception as e:
            st.warning(f"Could not load battery data: {e}")

    # Signal quality
    with col2:
        st.subheader("Signal quality")

        try:
            signal_df = execute_query(queries.SIGNAL_QUALITY_SUMMARY)

            if len(signal_df) > 0:
                # Problem rate chart
                fig = px.bar(
                    signal_df.sort_values("pct_problems", ascending=False),
                    x="course_id",
                    y=["pct_projected", "pct_problems"],
                    title="Signal issues by course",
                    labels={"value": "Percentage (%)", "course_id": "Course"},
                    barmode="group",
                    color_discrete_map={
                        "pct_projected": COLOR_WARNING,
                        "pct_problems": COLOR_CRITICAL,
                    },
                )
                fig.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=40, b=0),
                    legend_title_text="Issue type",
                )
                # Update legend labels
                fig.for_each_trace(
                    lambda t: t.update(
                        name="Projected" if t.name == "pct_projected" else "Problem"
                    )
                )
                st.plotly_chart(fig, use_container_width=True)

                # Explanation
                st.caption(
                    """
                **Projected**: GPS position was estimated (not actual fix)
                **Problem**: Data flagged as potentially unreliable
                """
                )

            else:
                st.info("No signal quality data available.")

        except Exception as e:
            st.warning(f"Could not load signal data: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Bottleneck analysis section
    # -------------------------------------------------------------------------
    st.header("Bottleneck analysis")
    st.markdown(
        """
    Bottlenecks occur when groups bunch up at specific locations on the course.
    We identify these using **pace gap** - the time spacing to the group ahead.
    
    - **High pace gap** = Groups are spread out (good flow)
    - **Low pace gap with high variability** = Groups bunching up (bottleneck)
    
    The map below shows hole-by-hole pace metrics, colour-coded by severity.
    """
    )

    # Course selector
    try:
        courses_df = execute_query(queries.COURSE_LIST)
        course_list = courses_df["course_id"].tolist()
    except Exception:
        course_list = []

    selected_course = st.selectbox(
        "Select course for bottleneck analysis",
        options=course_list if course_list else ["No courses available"],
        index=0,
    )

    if selected_course and selected_course != "No courses available":
        try:
            bottleneck_query = queries.get_bottleneck_query(selected_course)
            bottleneck_df = execute_query(bottleneck_query)

            if len(bottleneck_df) > 0 and bottleneck_df["lat"].notna().any():
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.subheader(f"Bottleneck map: {selected_course}")

                    # Prepare data for map
                    map_df = bottleneck_df[
                        bottleneck_df["lat"].notna() & bottleneck_df["lon"].notna()
                    ].copy()

                    # Calculate severity (higher pace gap stddev = more bottleneck)
                    if "pace_gap_stddev" in map_df.columns:
                        max_stddev = map_df["pace_gap_stddev"].max() or 1
                        map_df["severity"] = (
                            map_df["pace_gap_stddev"] / max_stddev * 100
                        )
                    else:
                        map_df["severity"] = 50

                    # Create hover text
                    map_df["hover_text"] = map_df.apply(
                        lambda r: f"<b>Hole {int(r['hole_number'])}</b><br>"
                        + f"Section: {int(r['section_number'])}<br>"
                        + f"Avg pace gap: {r['avg_pace_gap_seconds']:.2f}s<br>"
                        + f"Stddev: {r['pace_gap_stddev']:.2f}s<br>"
                        + f"Samples: {int(r['rounds_measured'])} rounds",
                        axis=1,
                    )

                    # Create scatter mapbox
                    fig = px.scatter_mapbox(
                        map_df,
                        lat="lat",
                        lon="lon",
                        color="avg_pace_gap_seconds",
                        size="rounds_measured",
                        hover_name="hover_text",
                        color_continuous_scale=COLOR_SCALE_REVERSE,
                        size_max=20,
                        zoom=15,
                        title=f"Pace gap by location - {selected_course}",
                    )

                    fig.update_layout(
                        mapbox_style="open-street-map",
                        height=500,
                        margin=dict(l=0, r=0, t=40, b=0),
                        coloraxis_colorbar=dict(
                            title="Pace gap (s)",
                        ),
                    )

                    # Center map on course
                    center_lat = map_df["lat"].mean()
                    center_lon = map_df["lon"].mean()
                    fig.update_layout(
                        mapbox=dict(
                            center=dict(lat=center_lat, lon=center_lon),
                            zoom=14,
                        )
                    )

                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.subheader("Hole-by-hole metrics")

                    # Summary table
                    summary_df = (
                        map_df.groupby("hole_number")
                        .agg(
                            {
                                "avg_pace_gap_seconds": "mean",
                                "pace_gap_stddev": "mean",
                                "rounds_measured": "sum",
                            }
                        )
                        .reset_index()
                    )
                    summary_df = summary_df.sort_values("hole_number")

                    # Add status indicator
                    def get_status(row):
                        if row["pace_gap_stddev"] > 60:
                            return "🔴 High variability"
                        elif row["pace_gap_stddev"] > 30:
                            return "🟡 Moderate"
                        else:
                            return "🟢 Good flow"

                    summary_df["status"] = summary_df.apply(get_status, axis=1)

                    st.dataframe(
                        summary_df[["hole_number", "avg_pace_gap_seconds", "status"]]
                        .rename(
                            columns={
                                "hole_number": "Hole",
                                "avg_pace_gap_seconds": "Avg gap (s)",
                            }
                        )
                        .style.format(
                            {
                                "Hole": "{:.0f}",
                                "Avg gap (s)": "{:.2f}",
                            }
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )

                    # Top bottlenecks
                    st.markdown("**Top bottleneck holes:**")
                    top_bottlenecks = summary_df.nlargest(3, "pace_gap_stddev")
                    for _, row in top_bottlenecks.iterrows():
                        st.markdown(
                            f"- Hole {int(row['hole_number'])}: {row['pace_gap_stddev']:.2f}s stddev"
                        )

                # Detailed analysis
                st.subheader("Pace gap distribution")

                col1, col2 = st.columns(2)

                with col1:
                    # Bar chart by hole
                    fig = px.bar(
                        map_df.sort_values("section_number"),
                        x="hole_number",
                        y="avg_pace_gap_seconds",
                        color="avg_pace_gap_seconds",
                        title="Average pace gap by hole",
                        labels={
                            "avg_pace_gap_seconds": "Pace gap (seconds)",
                            "hole_number": "Hole",
                        },
                        color_continuous_scale=COLOR_SCALE_REVERSE,
                    )
                    fig.update_layout(
                        height=350,
                        margin=dict(l=0, r=0, t=40, b=0),
                        showlegend=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    # Variability chart
                    fig = px.bar(
                        map_df.sort_values("section_number"),
                        x="hole_number",
                        y="pace_gap_stddev",
                        color="pace_gap_stddev",
                        title="Pace gap variability by hole",
                        labels={
                            "pace_gap_stddev": "Std deviation (seconds)",
                            "hole_number": "Hole",
                        },
                        color_continuous_scale=COLOR_SCALE_QUALITY,
                    )
                    fig.update_layout(
                        height=350,
                        margin=dict(l=0, r=0, t=40, b=0),
                        showlegend=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with st.expander("View raw bottleneck data"):
                    st.dataframe(
                        bottleneck_df.style.format(
                            {
                                "lat": "{:.6f}",
                                "lon": "{:.6f}",
                                "avg_pace_gap_seconds": "{:.2f}",
                                "pace_gap_stddev": "{:.2f}",
                                "avg_positional_gap": "{:.2f}",
                                "avg_pace_seconds": "{:.2f}",
                                "rounds_measured": "{:,.0f}",
                                "total_fixes": "{:,.0f}",
                            }
                        ),
                        use_container_width=True,
                    )

            else:
                st.warning(f"No GPS coordinate data available for {selected_course}.")
                st.info(
                    "Bottleneck analysis requires latitude and longitude values in the telemetry data."
                )

        except Exception as e:
            st.error(f"Failed to load bottleneck data: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Pace summary
    # -------------------------------------------------------------------------
    st.header("Pace summary by course")
    st.markdown(
        """
    Overview of average pace metrics across all rounds.
    """
    )

    try:
        pace_df = execute_query(queries.PACE_SUMMARY)

        if len(pace_df) > 0:
            # Aggregate by course
            pace_summary = (
                pace_df.groupby("course_id")
                .agg(
                    {
                        "round_id": "count",
                        "fix_count": "sum",
                        "avg_pace": "mean",
                        "avg_pace_gap": "mean",
                        "avg_positional_gap": "mean",
                    }
                )
                .reset_index()
            )
            pace_summary = pace_summary.rename(columns={"round_id": "rounds"})

            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    pace_summary.sort_values("avg_pace", ascending=False),
                    x="course_id",
                    y="avg_pace",
                    title="Average pace by course",
                    labels={"avg_pace": "Avg pace (seconds)", "course_id": "Course"},
                    color="avg_pace",
                    color_continuous_scale="RdYlGn_r",
                )
                fig.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.bar(
                    pace_summary.sort_values("avg_pace_gap", ascending=False),
                    x="course_id",
                    y="avg_pace_gap",
                    title="Average pace gap by course",
                    labels={
                        "avg_pace_gap": "Avg pace gap (seconds)",
                        "course_id": "Course",
                    },
                    color="avg_pace_gap",
                    color_continuous_scale=COLOR_SCALE_QUALITY,
                )
                fig.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

            with st.expander(
                "SQL (silver/dbt provenance: how gold is built)", expanded=False
            ):
                st.code(queries.DBT_PACE_SUMMARY, language="sql")

    except Exception as e:
        st.warning(f"Could not load pace summary: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Key insights
    # -------------------------------------------------------------------------
    st.header("Key insights")

    st.markdown(
        """
    **What pace metrics tell us:**
    
    - **Pace** measures how far behind goal time a group is (positive = slow)
    - **Pace gap** measures the time spacing to the group ahead
    - **Positional gap** measures the physical distance/position relative to where the group should be
    
    **Identifying bottlenecks:**
    
    1. Look for holes with **high pace gap standard deviation** - this indicates inconsistent flow
    2. Look for holes with **low average pace gap** combined with **high variability** - this signals bunching
    3. Use the map to visualise problem areas geographically
    
    **Recommendations:**
    
    - Holes with consistent bottlenecks may need operational adjustments (e.g., ranger intervention)
    - Device health issues can cause false bottleneck signals - always cross-reference with data quality
    """
    )

    st.markdown("---")
    render_dbt_models_section(
        [
            "pace_summary_by_round.sql",
            "signal_quality_rounds.sql",
            "device_health_errors.sql",
        ]
    )
