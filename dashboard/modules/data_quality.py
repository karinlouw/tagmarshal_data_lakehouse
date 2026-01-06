"""Data quality page.

Displays ingestion summary stats, column completeness heatmap, data quality scores,
and critical gaps analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.database import execute_query
from utils import queries
from utils.colors import (
    COLOR_SCALE_QUALITY,
    COLOR_GOOD,
    COLOR_WARNING,
    COLOR_CRITICAL,
)


def render():
    """Render the data quality page."""

    st.title("Data quality overview")
    st.markdown("Data completeness and quality metrics across all courses.")

    # -------------------------------------------------------------------------
    # Ingestion summary stats
    # -------------------------------------------------------------------------
    st.header("Ingestion summary")

    try:
        exec_df = execute_query(queries.EXECUTIVE_SUMMARY)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                label="Total courses",
                value=f"{exec_df['total_courses'].iloc[0]:,}",
            )

        with col2:
            st.metric(
                label="Total rounds",
                value=f"{exec_df['total_rounds'].iloc[0]:,}",
            )

        with col3:
            st.metric(
                label="Total telemetry events",
                value=f"{exec_df['total_events'].iloc[0]:,}",
            )

        with st.expander("SQL query"):
            st.code(queries.EXECUTIVE_SUMMARY, language="sql")

    except Exception as e:
        st.error(f"Failed to load ingestion summary: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Column completeness heatmap
    # -------------------------------------------------------------------------
    st.header("Column completeness")
    st.markdown("Data completeness percentage by course and column.")

    try:
        comp_df = execute_query(queries.COLUMN_COMPLETENESS)

        # Prepare data for heatmap
        heatmap_cols = [
            "pace_pct",
            "pace_gap_pct",
            "hole_pct",
            "section_pct",
            "gps_pct",
            "battery_pct",
            "start_hole_pct",
            "goal_time_pct",
            "timestamp_pct",
            "is_complete_pct",
        ]
        col_labels = [
            "Pace",
            "Pace gap",
            "Hole",
            "Section",
            "GPS",
            "Battery",
            "Start hole",
            "Goal time",
            "Start time",
            "Is complete",
        ]

        heatmap_data = comp_df.set_index("course_id")[heatmap_cols]
        heatmap_data.columns = col_labels

        fig = px.imshow(
            heatmap_data,
            labels=dict(x="Column", y="Course", color="Completeness (%)"),
            color_continuous_scale=COLOR_SCALE_QUALITY,
            range_color=[0, 100],
            aspect="auto",
            text_auto=".1f",
        )
        fig.update_layout(
            height=350,
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Create table with missing data (count and percentage)
        with st.expander("Completeness table", expanded=False):
            # Convert completeness percentages to numeric
            comp_display_df = comp_df.copy()
            for col in heatmap_cols:
                comp_display_df[col] = pd.to_numeric(
                    comp_display_df[col], errors="coerce"
                )

            comp_display_df["total"] = pd.to_numeric(
                comp_display_df["total"], errors="coerce"
            )

            # Build table data with count and percentage missing for each column
            table_data = []
            for _, row in comp_display_df.iterrows():
                course_row = {"course_id": row["course_id"]}
                total = row["total"]

                # For each column, calculate missing count and percentage
                for col_pct, col_label in zip(heatmap_cols, col_labels):
                    completeness_pct = row[col_pct]
                    missing_pct = (
                        100.0 - completeness_pct
                        if pd.notna(completeness_pct)
                        else 100.0
                    )
                    missing_count = (
                        int((missing_pct / 100.0) * total)
                        if pd.notna(missing_pct) and pd.notna(total)
                        else 0
                    )

                    # Add count and percentage columns
                    col_key = col_label.lower().replace(" ", "_")
                    course_row[f"{col_key}_count"] = missing_count
                    course_row[f"{col_key}_pct"] = missing_pct

                table_data.append(course_row)

            table_df = pd.DataFrame(table_data)

            # Build display columns: course_id, then count/pct pairs for each column
            display_cols = ["course_id"]
            format_dict = {}

            for col_label in col_labels:
                col_key = col_label.lower().replace(" ", "_")
                count_col = f"{col_key}_count"
                pct_col = f"{col_key}_pct"

                if count_col in table_df.columns and pct_col in table_df.columns:
                    display_cols.extend([count_col, pct_col])
                    format_dict[count_col] = "{:,}"
                    format_dict[pct_col] = "{:.3f}%"

            # Create readable column names
            rename_dict = {"course_id": "Course"}
            for col in display_cols[1:]:  # Skip course_id
                if col.endswith("_count"):
                    base_name = col.replace("_count", "").replace("_", " ").title()
                    rename_dict[col] = f"{base_name} (count)"
                elif col.endswith("_pct"):
                    base_name = col.replace("_pct", "").replace("_", " ").title()
                    rename_dict[col] = f"{base_name} (%)"

            table_display = table_df[display_cols].rename(columns=rename_dict)

            st.dataframe(
                table_display.style.format(format_dict),
                use_container_width=True,
            )

            st.caption(
                "Note: Missing data is calculated based on null values in the source data."
            )

        with st.expander("SQL query"):
            st.code(queries.COLUMN_COMPLETENESS, language="sql")

    except Exception as e:
        st.error(f"Failed to load column completeness: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Data quality scores and critical gaps analysis (combined)
    # -------------------------------------------------------------------------
    st.header("Data quality scores")
    st.markdown(
        """
        Quality score (0-100) based on importance tiers:
        - **Tier 1 (Pace)**: Pace and pace gaps (40%)
        - **Tier 2 (Location)**: Hole, section, GPS and start time (30%)
        - **Tier 3 (Device)**: Battery and cache (20%)
        - **Tier 4 (Config)**: Start hole, round type and goal time (10%)
        """
    )

    try:
        dq_df = execute_query(queries.DATA_QUALITY_OVERVIEW)
        gaps_df = execute_query(queries.CRITICAL_COLUMN_GAPS)

        # Convert numeric columns to proper types
        numeric_cols = [
            "total_events",
            "total_rounds",
            "pct_missing_pace",
            "pct_missing_pace_gap",
            "pct_missing_hole",
            "pct_missing_coords",
            "pct_missing_battery",
            "data_quality_score",
        ]
        for col in numeric_cols:
            if col in dq_df.columns:
                dq_df[col] = pd.to_numeric(dq_df[col], errors="coerce")

        # Create doughnut charts for each course
        def get_color_for_score(score):
            """Get color based on score using the same color profile."""
            if score >= 95:
                return COLOR_GOOD
            elif score >= 90:
                return "#2ecc71"
            elif score >= 85:
                return "#58d68d"
            elif score >= 80:
                return "#f1c40f"
            elif score >= 70:
                return COLOR_WARNING
            elif score >= 60:
                return "#f39c12"
            else:
                return COLOR_CRITICAL

        cols = st.columns(len(dq_df))
        for idx, (_, row) in enumerate(dq_df.iterrows()):
            with cols[idx]:
                score = row["data_quality_score"]
                course_name = row["course_id"]
                total_rounds = row["total_rounds"]
                total_events = row["total_events"]
                color = get_color_for_score(score)

                # Create doughnut chart
                fig = go.Figure(
                    data=[
                        go.Pie(
                            values=[score, 100 - score],
                            hole=0.7,
                            marker=dict(
                                colors=[color, "#e9ecef"],
                                line=dict(color="#ffffff", width=2),
                            ),
                            textinfo="none",
                            showlegend=False,
                        )
                    ]
                )

                fig.update_layout(
                    title=dict(
                        text=course_name,
                        font=dict(size=14, color="black"),
                        x=0.5,
                        xanchor="center",
                    ),
                    height=200,
                    margin=dict(l=0, r=0, t=40, b=0),
                    annotations=[
                        dict(
                            text=f"{score:.2f}%",
                            x=0.5,
                            y=0.5,
                            xanchor="center",
                            yanchor="middle",
                            font=dict(size=20, color="black", family="Arial Black"),
                            showarrow=False,
                        )
                    ],
                )

                st.plotly_chart(fig, use_container_width=True)
                st.markdown(
                    f'<p style="text-align: center; margin-top: -10px; font-size: 0.85em; color: #6c757d;">{total_rounds:,} rounds<br>{total_events:,} events</p>',
                    unsafe_allow_html=True,
                )

        # Detailed gaps table (tier filtered)
        st.subheader("Detailed gaps")
        tier_options = [
            "All",
            "1 Pace",
            "2 Location",
            "3 Device",
            "4 Config",
        ]
        selected_tier = st.radio(
            "Select tier to view:",
            tier_options,
            horizontal=True,
            index=0,
        )

        tier_columns = {
            "1 Pace": [
                ("Pace", "pct_null_pace"),
                ("Pace gap", "pct_null_pace_gap"),
                ("Positional gap", "pct_null_positional_gap"),
            ],
            "2 Location": [
                ("Hole", "pct_null_hole"),
                ("Section", "pct_null_section"),
                ("GPS", "pct_null_latitude"),
                ("Start time", "pct_null_timestamp"),
            ],
            "3 Device": [
                ("Battery", "pct_null_battery"),
                ("Cache", "pct_null_cache_flag"),
            ],
            "4 Config": [
                ("Start hole", "pct_null_start_hole"),
                ("Round type", "pct_null_nine_hole_flag"),
                ("Goal time", "pct_null_goal_time"),
            ],
        }

        if selected_tier == "All":
            selected_metrics = (
                tier_columns["1 Pace"]
                + tier_columns["2 Location"]
                + tier_columns["3 Device"]
                + tier_columns["4 Config"]
            )
        else:
            selected_metrics = tier_columns[selected_tier]

        gaps_display_df = gaps_df.copy()

        # Ensure numeric types
        gaps_display_df["total_events"] = pd.to_numeric(
            gaps_display_df["total_events"], errors="coerce"
        )
        gaps_display_df["usability_score"] = pd.to_numeric(
            gaps_display_df["usability_score"], errors="coerce"
        )
        for _, pct_col in selected_metrics:
            if pct_col in gaps_display_df.columns:
                gaps_display_df[pct_col] = pd.to_numeric(
                    gaps_display_df[pct_col], errors="coerce"
                )

        # Base columns
        display_cols = [
            "course_id",
        ]
        format_dict = {}

        # Add count/% pairs for the selected tier
        for label, pct_col in selected_metrics:
            count_col = f"count_missing__{pct_col}"
            if pct_col not in gaps_display_df.columns:
                continue

            gaps_display_df[count_col] = (
                (gaps_display_df[pct_col] / 100.0 * gaps_display_df["total_events"])
                .round()
                .fillna(0)
                .astype(int)
            )

            display_cols.extend([count_col, pct_col])
            format_dict[count_col] = "{:,}"
            format_dict[pct_col] = "{:.1f}%"

        # Rename columns for readability (simplified)
        rename_dict = {
            "course_id": "Course",
        }
        # Update format_dict to use renamed column names
        updated_format_dict = {}
        for label, pct_col in selected_metrics:
            count_col = f"count_missing__{pct_col}"
            # Simplify column names
            short_label = label.replace(" (GPS)", "").replace(" (nine-hole flag)", "")
            if count_col in gaps_display_df.columns:
                new_count_name = f"{short_label} (#)"
                rename_dict[count_col] = new_count_name
                updated_format_dict[new_count_name] = format_dict[count_col]
            if pct_col in gaps_display_df.columns:
                new_pct_name = f"{short_label} (%)"
                rename_dict[pct_col] = new_pct_name
                updated_format_dict[new_pct_name] = format_dict[pct_col]

        st.dataframe(
            gaps_display_df[display_cols]
            .rename(columns=rename_dict)
            .style.format(updated_format_dict),
            use_container_width=True,
        )

        # Status summary cards
        st.subheader("Status by tier")

        for _, row in gaps_df.iterrows():
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 3])

                with col1:
                    st.markdown(f"**{row['course_id']}**")
                    st.caption(f"Usability: {row['usability_score']:.3f}%")

                with col2:
                    st.markdown("**Pace**")
                    st.markdown(row["pace_data_status"])

                with col3:
                    st.markdown("**Location**")
                    st.markdown(row["location_data_status"])

                with col4:
                    st.markdown("**Device**")
                    st.markdown(row["device_health_status"])

                with col5:
                    st.markdown("**Recommendation**")
                    st.caption(row["top_recommendation"])

                st.markdown("---")

        with st.expander("SQL query"):
            st.code(queries.CRITICAL_COLUMN_GAPS, language="sql")

    except Exception as e:
        st.error(f"Failed to load data quality scores: {e}")

        # Status summary cards
        st.subheader("Status by tier")

        for _, row in gaps_df.iterrows():
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 3])

                with col1:
                    st.markdown(f"**{row['course_id']}**")
                    st.caption(f"Usability: {row['usability_score']:.3f}%")

                with col2:
                    st.markdown("**Pace**")
                    st.markdown(row["pace_data_status"])

                with col3:
                    st.markdown("**Location**")
                    st.markdown(row["location_data_status"])

                with col4:
                    st.markdown("**Device**")
                    st.markdown(row["device_health_status"])

                with col5:
                    st.markdown("**Recommendation**")
                    st.caption(row["top_recommendation"])

                st.markdown("---")
