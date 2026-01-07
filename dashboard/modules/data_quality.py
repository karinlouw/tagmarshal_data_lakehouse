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
from utils.dbt_provenance import render_dbt_models_section
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
                help="Excludes padding rows (real GPS fixes only)",
            )

        st.caption(
            "Courses + rounds are counted from `iceberg.gold.fact_rounds`; "
            "telemetry events are counted from `iceberg.silver.fact_telemetry_event` (non-padding)."
        )

    except Exception as e:
        st.error(f"Failed to load ingestion summary: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Column completeness heatmap
    # -------------------------------------------------------------------------
    st.header("Column completeness")
    st.markdown("Data completeness percentage by course and column.")

    try:
        # Add toggle for including/excluding padding
        include_padding = st.radio(
            "Include padding data:",
            ["Exclude padding (recommended)", "Include padding"],
            horizontal=True,
            index=0,
            key="completeness_padding_toggle",
        )

        use_padding = include_padding == "Include padding"

        if use_padding:
            comp_df = execute_query(queries.COLUMN_COMPLETENESS)
        else:
            comp_df = execute_query(queries.COLUMN_COMPLETENESS_NON_PADDING)

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
        st.subheader("Completeness table")
        try:
            missing_table_df = execute_query(queries.COLUMN_COMPLETENESS_MISSING_TABLE)
            st.dataframe(missing_table_df, use_container_width=True, hide_index=True)
            st.caption(
                "Note: this table is computed over **non-padding telemetry events** only "
                "(`is_location_padding = FALSE`). For round-level config fields like `start_hole`, "
                "missingness can look high because if a round is missing `start_hole`, it is NULL on "
                "every event in that round."
            )

            # Dropdown to show SQL query used for the table
            with st.expander("View SQL query", expanded=False):
                st.code(queries.COLUMN_COMPLETENESS_MISSING_TABLE, language="sql")
        except Exception as e:
            st.error(f"Failed to load completeness table: {e}")

    except Exception as e:
        st.error(f"Failed to load column completeness: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Padding data analysis
    # -------------------------------------------------------------------------
    st.header("Padding data analysis")

    # Explanation of padding rows
    with st.expander("ℹ️ What are padding rows?", expanded=True):
        st.markdown(
            """
        **Padding rows** are “empty slots” from TagMarshal’s fixed-size locations array.
        They are **not real telemetry**, and show up as rows where `hole_number` and `section_number` are NULL.

        **How to use them:**
        - Use `is_location_padding = FALSE` for analytics
        - Use padding only to understand file/schema shape and ingestion quirks
        """
        )

    st.markdown("Analysis of CSV padding rows and their impact on data completeness.")

    try:
        # Get padding statistics
        padding_stats_df = execute_query(queries.TELEMETRY_COMPLETENESS_SUMMARY)

        # Display summary metrics
        st.subheader("Padding overview")
        cols = st.columns(len(padding_stats_df))
        for idx, (_, row) in enumerate(padding_stats_df.iterrows()):
            with cols[idx]:
                course_name = row["course_id"]
                padding_pct = row["pct_padding_total"]
                padding_rows = int(row["padding_rows"])
                non_padding_rows = int(row["non_padding_rows"])

                st.metric(
                    label=course_name,
                    value=f"{padding_pct:.1f}%",
                    delta=f"{padding_rows:,} rows",
                    help=f"Total: {padding_rows:,} padding, {non_padding_rows:,} non-padding",
                )

        # Visualization: Padding vs non-padding breakdown
        st.subheader("Padding breakdown by course")

        # Prepare data for stacked bar chart
        padding_viz_data = []
        for _, row in padding_stats_df.iterrows():
            padding_viz_data.append(
                {
                    "course_id": row["course_id"],
                    "Type": "Padding rows",
                    "Count": int(row["padding_rows"]),
                    "Percentage": float(row["pct_padding_total"]),
                }
            )
            padding_viz_data.append(
                {
                    "course_id": row["course_id"],
                    "Type": "Non-padding rows",
                    "Count": int(row["non_padding_rows"]),
                    "Percentage": 100.0 - float(row["pct_padding_total"]),
                }
            )

        padding_viz_df = pd.DataFrame(padding_viz_data)

        fig_padding = px.bar(
            padding_viz_df,
            x="course_id",
            y="Count",
            color="Type",
            title="Row count: Padding vs Non-padding",
            labels={"course_id": "Course", "Count": "Number of rows"},
            color_discrete_map={
                "Padding rows": "#ff9800",
                "Non-padding rows": "#28a745",
            },
        )
        fig_padding.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=40, b=0),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(fig_padding, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load padding data analysis: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Critical gaps analysis
    # -------------------------------------------------------------------------
    st.header("Critical gaps analysis")

    try:
        gaps_df = execute_query(queries.CRITICAL_COLUMN_GAPS)

        # Detailed gaps table (tier filtered)
        st.subheader("Detailed gaps")
        tier_options = [
            "All tiers",
            "1. Pace",
            "2. Location",
            "3. Device",
            "4. Config",
        ]
        selected_tier = st.radio(
            "Select tier to view:",
            tier_options,
            horizontal=True,
            index=0,
        )

        tier_columns = {
            "1. Pace": [
                ("Pace", "pct_null_pace"),
                ("Pace gap", "pct_null_pace_gap"),
                ("Positional gap", "pct_null_positional_gap"),
            ],
            "2. Location": [
                ("Hole", "pct_null_hole"),
                ("Section", "pct_null_section"),
                ("GPS", "pct_null_latitude"),
                ("Start time", "pct_null_timestamp"),
            ],
            "3. Device": [
                ("Battery", "pct_null_battery"),
            ],
            "4. Config": [
                ("Start hole", "pct_null_start_hole"),
                ("Goal time", "pct_null_goal_time"),
            ],
        }

        if selected_tier == "All tiers":
            selected_metrics = (
                tier_columns["1. Pace"]
                + tier_columns["2. Location"]
                + tier_columns["3. Device"]
                + tier_columns["4. Config"]
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

        # Apply color coding to percentage columns
        def color_missing_pct(val):
            """Color code based on missing percentage."""
            if pd.isna(val):
                return ""
            try:
                pct = float(val)
                if pct == 0:
                    return f"background-color: {COLOR_GOOD}; color: white;"
                elif pct < 5:
                    return f"background-color: #90EE90; color: black;"  # Light green
                elif pct < 20:
                    return f"background-color: {COLOR_WARNING}; color: black;"  # Yellow/Amber
                elif pct < 50:
                    return f"background-color: #ff9800; color: white;"  # Orange
                else:
                    return f"background-color: {COLOR_CRITICAL}; color: white;"  # Red
            except (ValueError, TypeError):
                return ""

        # Create styled dataframe
        styled_df = (
            gaps_display_df[display_cols]
            .rename(columns=rename_dict)
            .style.format(updated_format_dict)
        )

        # Apply color to percentage columns
        for col in styled_df.columns:
            if " (%)" in col:
                styled_df = styled_df.applymap(color_missing_pct, subset=[col])

        st.dataframe(styled_df, use_container_width=True)

        # Course drill-down (simple)
        st.subheader("Course drill-down")
        st.caption("Missing values by field for each course.")

        # Convert required columns to numeric once
        numeric_cols_gaps = [
            "total_events",
            "pct_null_pace",
            "pct_null_pace_gap",
            "pct_null_positional_gap",
            "pct_null_hole",
            "pct_null_section",
            "pct_null_latitude",
            "pct_null_timestamp",
            "pct_null_battery",
            "pct_null_start_hole",
            "pct_null_goal_time",
        ]
        for col in numeric_cols_gaps:
            if col in gaps_df.columns:
                gaps_df[col] = pd.to_numeric(gaps_df[col], errors="coerce")

        metric_map = [
            ("1. Pace", "Pace", "pct_null_pace"),
            ("1. Pace", "Pace gap", "pct_null_pace_gap"),
            ("1. Pace", "Positional gap", "pct_null_positional_gap"),
            ("2. Location", "Hole", "pct_null_hole"),
            ("2. Location", "Section", "pct_null_section"),
            ("2. Location", "GPS", "pct_null_latitude"),
            ("2. Location", "Start time", "pct_null_timestamp"),
            ("3. Device", "Battery", "pct_null_battery"),
            ("4. Config", "Start hole", "pct_null_start_hole"),
            ("4. Config", "Goal time", "pct_null_goal_time"),
        ]

        course_ids = gaps_df["course_id"].astype(str).tolist()
        tabs = st.tabs(course_ids)
        for i, course_id in enumerate(course_ids):
            with tabs[i]:
                course_row = gaps_df[
                    gaps_df["course_id"].astype(str) == course_id
                ].iloc[0]
                total_events = course_row.get("total_events", 0)
                if pd.isna(total_events):
                    total_events = 0

                rows = []
                for tier_name, field_name, pct_col in metric_map:
                    pct = course_row.get(pct_col, 0)
                    if pd.isna(pct):
                        pct = 0
                    missing_count = (
                        int(round((float(pct) / 100.0) * float(total_events)))
                        if total_events
                        else 0
                    )
                    rows.append(
                        {
                            "Tier": tier_name,
                            "Field": field_name,
                            "Missing (#)": missing_count,
                            "Missing (%)": float(pct),
                        }
                    )

                course_df = (
                    pd.DataFrame(rows)
                    .sort_values(
                        ["Missing (%)", "Missing (#)"], ascending=[False, False]
                    )
                    .head(5)
                )

                def colour_pct(val):
                    pct_val = float(val)
                    if pct_val >= 20:
                        return f"background-color: {COLOR_CRITICAL}; color: white;"
                    if pct_val >= 5:
                        return f"background-color: {COLOR_WARNING}; color: black;"
                    if pct_val > 0:
                        return "background-color: #90EE90; color: black;"
                    return f"background-color: {COLOR_GOOD}; color: white;"

                styled_course = course_df.style.format(
                    {
                        "Missing (#)": "{:,}",
                        "Missing (%)": "{:.1f}%",
                    }
                ).applymap(colour_pct, subset=["Missing (%)"])
                st.dataframe(
                    styled_course, use_container_width=True, hide_index=True, height=300
                )

    except Exception as e:
        st.error(f"Failed to load data quality scores: {e}")

    st.markdown("---")
    render_dbt_models_section(
        [
            "data_quality_overview.sql",
            "critical_column_gaps.sql",
        ]
    )
