-- global_overview (Silver-normalized)
-- -----------------------------------------------------------------------------
-- Grain: single row
-- Purpose:
--   Global KPI summary across all Silver telemetry. This is ideal for demos
--   to show "everything is accessible in one place" without jumping between
--   many tables.
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        course_id,
        round_id,
        device AS device_id,
        fix_timestamp,
        is_timestamp_missing,
        is_location_padding,
        is_problem,
        is_complete,
        is_nine_hole,
        is_secondary,
        is_auto_assigned,
        pace_gap
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE course_id IS NOT NULL
),

agg AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN is_location_padding THEN 1 ELSE 0 END) AS padding_rows,
        SUM(CASE WHEN NOT is_location_padding THEN 1 ELSE 0 END) AS non_padding_rows,

        COUNT(DISTINCT course_id) AS courses,
        COUNT(DISTINCT round_id) AS distinct_rounds_all,
        COUNT(DISTINCT CASE WHEN NOT is_location_padding THEN round_id END) AS distinct_rounds_non_padding,
        COUNT(DISTINCT device_id) AS distinct_devices,

        SUM(CASE WHEN is_timestamp_missing THEN 1 ELSE 0 END) AS ts_missing_rows,
        SUM(CASE WHEN is_timestamp_missing AND NOT is_location_padding THEN 1 ELSE 0 END) AS ts_missing_non_padding_rows,

        SUM(CASE WHEN NOT is_location_padding AND is_problem THEN 1 ELSE 0 END) AS problem_rows,

        COUNT(DISTINCT CASE WHEN NOT is_location_padding AND COALESCE(is_complete, FALSE) THEN round_id END) AS complete_rounds,
        COUNT(DISTINCT CASE WHEN NOT is_location_padding AND COALESCE(is_nine_hole, FALSE) THEN round_id END) AS nine_hole_rounds,
        COUNT(DISTINCT CASE WHEN NOT is_location_padding AND COALESCE(is_secondary, FALSE) THEN round_id END) AS secondary_rounds,
        COUNT(DISTINCT CASE WHEN NOT is_location_padding AND COALESCE(is_auto_assigned, FALSE) THEN round_id END) AS auto_assigned_rounds,

        MIN(fix_timestamp) AS first_fix_ts,
        MAX(fix_timestamp) AS last_fix_ts,

        AVG(pace_gap) FILTER (WHERE NOT is_location_padding) AS avg_pace_gap_sec,
        APPROX_PERCENTILE(pace_gap, 0.5) FILTER (WHERE NOT is_location_padding) AS median_pace_gap_sec
    FROM base
)

SELECT
    total_rows,
    padding_rows,
    non_padding_rows,
    courses,
    distinct_rounds_all,
    distinct_rounds_non_padding,
    distinct_devices,

    ts_missing_rows,
    ts_missing_non_padding_rows,

    problem_rows,
    ROUND(100.0 * problem_rows / NULLIF(non_padding_rows, 0), 2) AS pct_problem_rows_non_padding,

    complete_rounds,
    ROUND(100.0 * complete_rounds / NULLIF(distinct_rounds_non_padding, 0), 2) AS pct_complete_rounds,

    nine_hole_rounds,
    ROUND(100.0 * nine_hole_rounds / NULLIF(distinct_rounds_non_padding, 0), 2) AS pct_nine_hole_rounds,

    secondary_rounds,
    ROUND(100.0 * secondary_rounds / NULLIF(distinct_rounds_non_padding, 0), 2) AS pct_secondary_rounds,

    auto_assigned_rounds,
    ROUND(100.0 * auto_assigned_rounds / NULLIF(distinct_rounds_non_padding, 0), 2) AS pct_auto_assigned_rounds,

    ROUND(100.0 * padding_rows / NULLIF(total_rows, 0), 2) AS pct_padding_rows,
    ROUND(100.0 * ts_missing_rows / NULLIF(total_rows, 0), 2) AS pct_ts_missing_rows,
    ROUND(100.0 * ts_missing_non_padding_rows / NULLIF(non_padding_rows, 0), 2) AS pct_ts_missing_non_padding_rows,

    first_fix_ts,
    last_fix_ts,
    avg_pace_gap_sec,
    median_pace_gap_sec
FROM agg


