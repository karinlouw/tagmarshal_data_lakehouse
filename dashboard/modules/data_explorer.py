"""Data explorer page.

Interactive exploration of Silver/Gold tables:
- select a table + column(s)
- optional course filter (course_id / facility_id)
- view summary stats + value distributions

Also includes a preset "round duration" explorer for telemetry-style data.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.database import execute_query


@dataclass(frozen=True)
class TableRef:
    label: str
    fqtn: str  # fully qualified table name

    @property
    def schema(self) -> str:
        # iceberg.<schema>.<table>
        parts = self.fqtn.split(".")
        if len(parts) != 3:
            raise ValueError(f"Unexpected table reference: {self.fqtn}")
        return parts[1]

    @property
    def table(self) -> str:
        parts = self.fqtn.split(".")
        if len(parts) != 3:
            raise ValueError(f"Unexpected table reference: {self.fqtn}")
        return parts[2]


TABLE_OPTIONS: list[TableRef] = [
    TableRef("Silver · Telemetry events", "iceberg.silver.fact_telemetry_event"),
    TableRef("Silver · Facility topology", "iceberg.silver.dim_facility_topology"),
    TableRef("Gold · Course configuration", "iceberg.gold.course_configuration_analysis"),
    TableRef("Gold · Course dimension", "iceberg.gold.dim_course"),
]


def _quote_ident(name: str) -> str:
    # We mostly use metadata-derived column names, but keep this safe/robust.
    return '"' + name.replace('"', '""') + '"'


def _is_numeric(data_type: str) -> bool:
    dt = (data_type or "").lower()
    return any(
        k in dt
        for k in (
            "tinyint",
            "smallint",
            "integer",
            "int",
            "bigint",
            "double",
            "real",
            "decimal",
        )
    )


def _is_timestampish(data_type: str) -> bool:
    dt = (data_type or "").lower()
    return ("timestamp" in dt) or (dt == "date")


@st.cache_data(ttl=300)
def _get_columns(schema: str, table: str) -> pd.DataFrame:
    q = f"""
    SELECT
      column_name,
      data_type,
      ordinal_position
    FROM iceberg.information_schema.columns
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    return execute_query(q)


@st.cache_data(ttl=300)
def _get_distinct_values(fqtn: str, col: str, limit: int = 200) -> list[str]:
    # Keep it bounded; this dashboard is intended for exploratory use.
    q = f"""
    SELECT DISTINCT CAST({_quote_ident(col)} AS VARCHAR) AS v
    FROM {fqtn}
    WHERE {_quote_ident(col)} IS NOT NULL
    ORDER BY v
    LIMIT {int(limit)}
    """
    df = execute_query(q)
    return [str(x) for x in df["v"].tolist()]


def _build_where_clause(
    course_col: str | None,
    selected_courses: list[str],
    extra_filters: list[str],
) -> str:
    clauses: list[str] = []
    if course_col and selected_courses:
        values = ", ".join("'" + c.replace("'", "''") + "'" for c in selected_courses)
        clauses.append(f"{_quote_ident(course_col)} IN ({values})")
    clauses.extend([f for f in extra_filters if f])
    return " AND ".join(clauses) if clauses else "1=1"


def _render_distribution(
    fqtn: str,
    col_name: str,
    data_type: str,
    where_sql: str,
):
    st.subheader("Distribution")

    col_sql = _quote_ident(col_name)

    # Timestamp-ish: group by day/hour
    if _is_timestampish(data_type):
        granularity = st.selectbox("Time bucket", ["day", "hour"], index=0)
        trunc_unit = "day" if granularity == "day" else "hour"
        q = f"""
        SELECT
          date_trunc('{trunc_unit}', {col_sql}) AS bucket,
          COUNT(*) AS n
        FROM {fqtn}
        WHERE {where_sql}
          AND {col_sql} IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
        df = execute_query(q)
        if df.empty:
            st.info("No non-null values for this column under the current filters.")
            return
        fig = px.bar(df, x="bucket", y="n", title=f"{col_name} by {granularity}")
        fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)
        return

    # Numeric: histogram via width_bucket
    if _is_numeric(data_type):
        bins = st.slider("Bins", min_value=5, max_value=80, value=30, step=5)

        stats_q = f"""
        SELECT
          MIN(CAST({col_sql} AS DOUBLE)) AS min_x,
          MAX(CAST({col_sql} AS DOUBLE)) AS max_x,
          COUNT(*) AS n_rows,
          SUM(CASE WHEN {col_sql} IS NULL THEN 1 ELSE 0 END) AS n_null
        FROM {fqtn}
        WHERE {where_sql}
        """
        stats = execute_query(stats_q).iloc[0].to_dict()

        if stats["min_x"] is None or stats["max_x"] is None:
            st.info("No non-null values for this column under the current filters.")
            return

        min_x = float(stats["min_x"])
        max_x = float(stats["max_x"])
        if min_x == max_x:
            st.metric("Unique value", f"{min_x:g}")
            st.caption("All non-null values are identical; histogram is not meaningful.")
            return

        bucket_q = f"""
        WITH base AS (
          SELECT CAST({col_sql} AS DOUBLE) AS x
          FROM {fqtn}
          WHERE {where_sql}
            AND {col_sql} IS NOT NULL
        ),
        bounds AS (
          SELECT {min_x} AS min_x, {max_x} AS max_x
        ),
        binned AS (
          SELECT width_bucket(x, min_x, max_x, {int(bins)}) AS bin
          FROM base
          CROSS JOIN bounds
        )
        SELECT bin, COUNT(*) AS n
        FROM binned
        GROUP BY 1
        ORDER BY 1
        """
        df = execute_query(bucket_q)
        width = (max_x - min_x) / float(bins)

        def _label(b: int) -> str:
            lo = min_x + (b - 1) * width
            hi = min_x + b * width
            return f"[{lo:g}, {hi:g})"

        df["range"] = df["bin"].astype(int).map(_label)
        fig = px.bar(df, x="range", y="n", title=f"{col_name} (binned)")
        fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        return

    # Categorical-ish: top values
    top_n = st.slider("Top values", min_value=10, max_value=200, value=30, step=10)
    q = f"""
    SELECT
      CAST({col_sql} AS VARCHAR) AS value,
      COUNT(*) AS n
    FROM {fqtn}
    WHERE {where_sql}
    GROUP BY 1
    ORDER BY n DESC
    LIMIT {int(top_n)}
    """
    df = execute_query(q)
    if df.empty:
        st.info("No values for this column under the current filters.")
        return
    fig = px.bar(df, x="value", y="n", title=f"Top values: {col_name}")
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)


def _render_round_duration_preset(course_id: str | None):
    st.subheader("Preset: rounds with NULL start_hole (duration)")
    st.caption(
        "Aggregates telemetry to round-level using MIN/MAX(fix_timestamp) and highlights long-duration sessions."
    )

    course = st.text_input(
        "course_id",
        value=course_id or "bradshawfarmgc",
        help="Defaults to Bradshaw; you can change this to any course_id.",
    )
    limit = st.slider("Max rounds", min_value=50, max_value=2000, value=300, step=50)

    q = f"""
    WITH base AS (
      SELECT *
      FROM iceberg.silver.fact_telemetry_event
      WHERE course_id = '{course.replace("'", "''")}'
        AND is_location_padding = FALSE
    ),
    rounds AS (
      SELECT
        round_id,
        MAX(start_hole) AS start_hole_any,
        COUNT(*) AS non_padding_rows,
        MIN(fix_timestamp) AS first_fix_ts,
        MAX(fix_timestamp) AS last_fix_ts,
        DATE_DIFF('second', MIN(fix_timestamp), MAX(fix_timestamp)) AS duration_sec,
        COUNT(DISTINCT hole_number) AS holes_observed,
        COUNT(DISTINCT section_number) AS sections_observed,
        SUM(CASE WHEN is_timestamp_missing THEN 1 ELSE 0 END) AS ts_missing_rows,
        SUM(CASE WHEN is_problem THEN 1 ELSE 0 END) AS problem_rows,
        SUM(CASE WHEN is_projected THEN 1 ELSE 0 END) AS projected_rows,
        MAX(is_complete) AS is_complete_any,
        MIN(round_start_time) AS round_start_time_any,
        MAX(round_end_time) AS round_end_time_any,
        MAX(device) AS device_any
      FROM base
      GROUP BY round_id
      HAVING MAX(start_hole) IS NULL
    )
    SELECT *
    FROM rounds
    ORDER BY duration_sec DESC NULLS LAST, non_padding_rows DESC
    LIMIT {int(limit)}
    """
    df = execute_query(q)
    st.dataframe(df, use_container_width=True, hide_index=True)

    if df.empty:
        return

    st.markdown("#### Inspect a specific round")
    # Keep the dropdown bounded for UI responsiveness
    round_options = df["round_id"].head(300).tolist()
    selected_round = st.selectbox("round_id", options=round_options)

    events_q = f"""
    SELECT
      fix_timestamp,
      hole_number,
      section_number,
      is_cache,
      is_projected,
      is_problem,
      is_timestamp_missing,
      round_start_time,
      round_end_time,
      start_hole,
      start_section,
      end_section,
      device
    FROM iceberg.silver.fact_telemetry_event
    WHERE course_id = '{course.replace("'", "''")}'
      AND round_id = '{str(selected_round).replace("'", "''")}'
      AND is_location_padding = FALSE
    ORDER BY fix_timestamp NULLS LAST
    LIMIT 5000
    """
    ev = execute_query(events_q)
    if ev.empty:
        st.info("No non-padding events found for that round.")
        return

    # Compute gaps (pandas)
    ev_ts = ev.dropna(subset=["fix_timestamp"]).copy()
    if not ev_ts.empty:
        ev_ts = ev_ts.sort_values("fix_timestamp")
        ev_ts["gap_sec"] = (
            ev_ts["fix_timestamp"].diff().dt.total_seconds()
        )
        max_gap = ev_ts["gap_sec"].max(skipna=True)
        gap_over_1h = int((ev_ts["gap_sec"] > 3600).sum())
        c1, c2, c3 = st.columns(3)
        c1.metric("Events (non-padding)", f"{len(ev):,}")
        c2.metric("Max gap (sec)", f"{0 if pd.isna(max_gap) else int(max_gap):,}")
        c3.metric("Gaps > 1h", f"{gap_over_1h:,}")

        with st.expander("Gap detail (largest gaps)"):
            st.dataframe(
                ev_ts.sort_values("gap_sec", ascending=False).head(50),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.metric("Events (non-padding)", f"{len(ev):,}")
        st.caption("All fix_timestamp values are NULL for this round under the current filters.")

    with st.expander("Event sample"):
        st.dataframe(ev.head(200), use_container_width=True, hide_index=True)


def render():
    st.title("Data explorer")
    st.caption("Explore distributions and anomalies across Silver/Gold tables")

    st.markdown("---")

    selected = st.selectbox(
        "Table",
        options=TABLE_OPTIONS,
        format_func=lambda t: t.label,
    )

    cols_df = _get_columns(selected.schema, selected.table)
    if cols_df.empty:
        st.warning("Could not load columns for this table.")
        return

    col_names = cols_df["column_name"].tolist()
    col_type_map = dict(zip(cols_df["column_name"], cols_df["data_type"]))

    # Detect common course dimension columns
    course_col = None
    if "course_id" in col_names:
        course_col = "course_id"
    elif "facility_id" in col_names:
        course_col = "facility_id"

    with st.sidebar:
        st.subheader("Filters")

        selected_courses: list[str] = []
        if course_col:
            course_values = _get_distinct_values(selected.fqtn, course_col, limit=200)
            selected_courses = st.multiselect(
                f"{course_col}",
                options=course_values,
                default=["bradshawfarmgc"] if "bradshawfarmgc" in course_values else [],
            )

        extra_filters: list[str] = []
        if "is_location_padding" in col_names:
            non_padding_only = st.toggle("Non-padding only", value=True)
            if non_padding_only:
                extra_filters.append(f"{_quote_ident('is_location_padding')} = FALSE")

        where_sql = _build_where_clause(course_col, selected_courses, extra_filters)

    st.subheader("Column selection")
    selected_col = st.selectbox("Distribution column", options=col_names, index=0)
    dtype = col_type_map.get(selected_col, "")

    # Quick summary
    st.subheader("Summary stats")
    summary_q = f"""
    SELECT
      COUNT(*) AS rows_total,
      SUM(CASE WHEN {_quote_ident(selected_col)} IS NULL THEN 1 ELSE 0 END) AS nulls,
      APPROX_DISTINCT({_quote_ident(selected_col)}) AS approx_distinct
    FROM {selected.fqtn}
    WHERE {where_sql}
    """
    summary = execute_query(summary_q).iloc[0].to_dict()
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", f"{int(summary['rows_total']):,}")
    c2.metric("Nulls", f"{int(summary['nulls']):,}")
    c3.metric("Approx distinct", f"{int(summary['approx_distinct']):,}")

    st.markdown("---")
    _render_distribution(selected.fqtn, selected_col, dtype, where_sql)

    st.markdown("---")
    with st.expander("Show a sample of rows"):
        sample_limit = st.slider("Sample rows", min_value=10, max_value=500, value=50, step=10)
        q = f"SELECT * FROM {selected.fqtn} WHERE {where_sql} LIMIT {int(sample_limit)}"
        st.dataframe(execute_query(q), use_container_width=True)

    # Telemetry-focused preset (ties directly to your investigation)
    if selected.fqtn == "iceberg.silver.fact_telemetry_event":
        st.markdown("---")
        _render_round_duration_preset(
            selected_courses[0] if selected_courses else None
        )


