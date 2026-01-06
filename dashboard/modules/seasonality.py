"""Seasonality page.

Displays round volume patterns by month and weekday, helping to understand
peak periods and seasonal trends.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.database import execute_query
from utils import queries
from utils.colors import (
    COLOR_SEQUENCE,
    COLOR_MAP_DAY_TYPE,
    COLOR_SCALE_QUALITY,
)


def render():
    """Render the seasonality page."""

    st.title("Seasonality analysis")
    st.markdown(
        """
    This page shows volume patterns across time, helping to identify peak seasons 
    and busy days. Understanding seasonality is important for capacity planning 
    and performance benchmarking.
    """
    )

    # -------------------------------------------------------------------------
    # Course filter
    # -------------------------------------------------------------------------
    try:
        courses_df = execute_query(queries.COURSE_LIST)
        course_list = courses_df["course_id"].tolist()
    except Exception:
        course_list = []

    selected_courses = st.multiselect(
        "Filter by course",
        options=course_list,
        default=course_list,
        help="Select courses to include in the analysis",
    )

    if not selected_courses:
        st.warning("Please select at least one course.")
        return

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Monthly distribution
    # -------------------------------------------------------------------------
    st.header("Rounds by month")
    st.markdown(
        """
    Monthly round distribution shows seasonal patterns. Golf courses typically 
    see higher volumes during favourable weather months.
    """
    )

    try:
        monthly_df = execute_query(queries.ROUNDS_BY_MONTH)
        monthly_df = monthly_df[monthly_df["course_id"].isin(selected_courses)]

        if len(monthly_df) > 0:
            # Overall monthly trend (all courses combined)
            col1, col2 = st.columns(2)

            with col1:
                # Stacked bar by course
                fig = px.bar(
                    monthly_df,
                    x="month_name",
                    y="rounds",
                    color="course_id",
                    title="Rounds by month (by course)",
                    labels={
                        "rounds": "Rounds",
                        "month_name": "Month",
                        "course_id": "Course",
                    },
                    category_orders={
                        "month_name": [
                            "January",
                            "February",
                            "March",
                            "April",
                            "May",
                            "June",
                            "July",
                            "August",
                            "September",
                            "October",
                            "November",
                            "December",
                        ]
                    },
                    color_discrete_sequence=COLOR_SEQUENCE,
                )
                fig.update_layout(
                    height=400,
                    margin=dict(l=0, r=0, t=40, b=0),
                    xaxis_tickangle=-45,
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Heatmap by course and month
                pivot_df = monthly_df.pivot_table(
                    index="course_id",
                    columns="month_name",
                    values="rounds",
                    fill_value=0,
                ).reindex(
                    columns=[
                        "January",
                        "February",
                        "March",
                        "April",
                        "May",
                        "June",
                        "July",
                        "August",
                        "September",
                        "October",
                        "November",
                        "December",
                    ],
                    fill_value=0,
                )

                fig = px.imshow(
                    pivot_df,
                    labels=dict(x="Month", y="Course", color="Rounds"),
                    color_continuous_scale=COLOR_SCALE_QUALITY,
                    aspect="auto",
                    title="Monthly volume heatmap",
                )
                fig.update_layout(
                    height=400,
                    margin=dict(l=0, r=0, t=40, b=0),
                    xaxis_tickangle=-45,
                )
                st.plotly_chart(fig, use_container_width=True)

            # Per-course breakdown
            st.subheader("Peak months by course")

            cols = st.columns(min(len(selected_courses), 3))
            for idx, course in enumerate(selected_courses):
                course_monthly = monthly_df[monthly_df["course_id"] == course].copy()
                if len(course_monthly) > 0:
                    peak_month = course_monthly.loc[course_monthly["rounds"].idxmax()]
                    low_month = course_monthly.loc[course_monthly["rounds"].idxmin()]
                    total_rounds = course_monthly["rounds"].sum()

                    with cols[idx % 3]:
                        st.markdown(f"**{course}**")
                        st.metric(
                            label="Peak month",
                            value=peak_month["month_name"],
                            delta=f"{peak_month['pct_total']:.2f}% of total",
                        )
                        st.caption(
                            f"Lowest: {low_month['month_name']} ({low_month['pct_total']:.2f}%)"
                        )
                        st.caption(f"Total: {total_rounds:,} rounds")

            with st.expander("SQL query"):
                st.code(queries.ROUNDS_BY_MONTH, language="sql")

            with st.expander("View monthly data table"):
                display_df = monthly_df[
                    ["course_id", "month_name", "rounds", "pct_total"]
                ].copy()
                st.dataframe(
                    display_df.style.format(
                        {
                            "rounds": "{:,.0f}",
                            "pct_total": "{:.2f}%",
                        }
                    ),
                    use_container_width=True,
                )
        else:
            st.info("No monthly data available for selected courses.")

    except Exception as e:
        st.error(f"Failed to load monthly data: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Weekday distribution
    # -------------------------------------------------------------------------
    st.header("Rounds by weekday")
    st.markdown(
        """
    Weekday distribution shows which days are busiest. Most courses see higher 
    volumes on weekends.
    """
    )

    try:
        weekday_df = execute_query(queries.ROUNDS_BY_WEEKDAY)
        weekday_df = weekday_df[weekday_df["course_id"].isin(selected_courses)]

        if len(weekday_df) > 0:
            col1, col2 = st.columns(2)

            weekday_order = [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]

            with col1:
                # Grouped bar by course
                fig = px.bar(
                    weekday_df,
                    x="weekday_name",
                    y="rounds",
                    color="course_id",
                    title="Rounds by weekday (by course)",
                    labels={
                        "rounds": "Rounds",
                        "weekday_name": "Day",
                        "course_id": "Course",
                    },
                    category_orders={"weekday_name": weekday_order},
                    barmode="group",
                    color_discrete_sequence=COLOR_SEQUENCE,
                )
                fig.update_layout(
                    height=400,
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Combined weekday totals
                combined_weekday = (
                    weekday_df.groupby("weekday_name")["rounds"].sum().reset_index()
                )
                combined_weekday["weekday_order"] = combined_weekday[
                    "weekday_name"
                ].map({day: i for i, day in enumerate(weekday_order)})
                combined_weekday = combined_weekday.sort_values("weekday_order")

                fig = px.bar(
                    combined_weekday,
                    x="weekday_name",
                    y="rounds",
                    title="Total rounds by weekday (all courses)",
                    labels={"rounds": "Rounds", "weekday_name": "Day"},
                    color="rounds",
                    color_continuous_scale=COLOR_SCALE_QUALITY,
                )
                fig.update_layout(
                    height=400,
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

            # Weekend vs weekday comparison
            st.subheader("Weekend vs weekday")

            weekday_df["is_weekend"] = weekday_df["weekday_name"].isin(
                ["Saturday", "Sunday"]
            )
            weekend_comparison = (
                weekday_df.groupby(["course_id", "is_weekend"])["rounds"]
                .sum()
                .reset_index()
            )
            weekend_comparison["day_type"] = weekend_comparison["is_weekend"].map(
                {True: "Weekend", False: "Weekday"}
            )

            fig = px.bar(
                weekend_comparison,
                x="course_id",
                y="rounds",
                color="day_type",
                title="Weekend vs weekday rounds by course",
                labels={
                    "rounds": "Rounds",
                    "course_id": "Course",
                    "day_type": "Day type",
                },
                barmode="group",
                color_discrete_map=COLOR_MAP_DAY_TYPE,
            )
            fig.update_layout(
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("SQL query"):
                st.code(queries.ROUNDS_BY_WEEKDAY, language="sql")

            with st.expander("View weekday data table"):
                display_df = weekday_df[["course_id", "weekday_name", "rounds"]].copy()
                st.dataframe(
                    display_df.style.format({"rounds": "{:,.0f}"}),
                    use_container_width=True,
                )
        else:
            st.info("No weekday data available for selected courses.")

    except Exception as e:
        st.error(f"Failed to load weekday data: {e}")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Key insights
    # -------------------------------------------------------------------------
    st.header("Key insights")

    try:
        monthly_df = execute_query(queries.ROUNDS_BY_MONTH)
        monthly_df = monthly_df[monthly_df["course_id"].isin(selected_courses)]

        weekday_df = execute_query(queries.ROUNDS_BY_WEEKDAY)
        weekday_df = weekday_df[weekday_df["course_id"].isin(selected_courses)]

        if len(monthly_df) > 0 and len(weekday_df) > 0:
            col1, col2 = st.columns(2)

            with col1:
                # Peak season
                total_by_month = monthly_df.groupby("month_name")["rounds"].sum()
                peak_month = total_by_month.idxmax()
                peak_rounds = total_by_month.max()
                total_rounds = total_by_month.sum()

                st.info(
                    f"""
                **Peak season:** {peak_month}
                
                {peak_rounds:,} rounds ({100 * peak_rounds / total_rounds:.2f}% of total)
                """
                )

            with col2:
                # Busiest day
                total_by_weekday = weekday_df.groupby("weekday_name")["rounds"].sum()
                busiest_day = total_by_weekday.idxmax()
                busiest_rounds = total_by_weekday.max()
                total_rounds = total_by_weekday.sum()

                st.info(
                    f"""
                **Busiest day:** {busiest_day}
                
                {busiest_rounds:,} rounds ({100 * busiest_rounds / total_rounds:.2f}% of weekly total)
                """
                )
    except Exception:
        pass
