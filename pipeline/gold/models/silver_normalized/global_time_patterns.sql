-- global_time_patterns (Silver-normalized)
-- -----------------------------------------------------------------------------
-- Grain: (course_id, day_type, time_bucket)
-- Purpose:
--   A demo-friendly table to explore time-of-day and weekday/weekend effects,
--   using only signals we actually have: fix timestamps + is_problem + pace_gap.
--
-- time_bucket:
--   - 'morning'   = 05:00–11:59 UTC
--   - 'afternoon' = 12:00–17:59 UTC
--   - 'evening'   = 18:00–23:59 UTC
--   - 'night'     = 00:00–04:59 UTC
--   - 'unknown'   = missing timestamps
--
-- NOTE: All timestamps are standardized to UTC for global cross-course analysis.
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH fixes AS (
    SELECT
        course_id,
        round_id,
        fix_timestamp,
        -- Convert to UTC for standardized global analysis
        fix_timestamp AT TIME ZONE 'UTC' AS fix_timestamp_utc,
        is_timestamp_missing,
        is_location_padding,
        is_problem,
        pace_gap
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE course_id IS NOT NULL
      AND NOT is_location_padding
),

bucketed AS (
    SELECT
        course_id,
        round_id,
        fix_timestamp,
        is_timestamp_missing,
        is_problem,
        pace_gap,
        CASE
            WHEN fix_timestamp IS NULL THEN 'unknown'
            WHEN hour(fix_timestamp_utc) BETWEEN 5 AND 11 THEN 'morning'
            WHEN hour(fix_timestamp_utc) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN hour(fix_timestamp_utc) BETWEEN 18 AND 23 THEN 'evening'
            ELSE 'night'
        END AS time_bucket,
        CASE
            WHEN fix_timestamp IS NULL THEN 'unknown'
            WHEN day_of_week(fix_timestamp_utc) IN (6, 7) THEN 'weekend'
            ELSE 'weekday'
        END AS day_type,
        CASE
            WHEN fix_timestamp IS NULL THEN NULL
            ELSE hour(fix_timestamp_utc)
        END AS hour_of_day
    FROM fixes
),

agg AS (
    SELECT
        course_id,
        day_type,
        time_bucket,
        hour_of_day,
        COUNT(*) AS rows,
        COUNT(DISTINCT round_id) AS rounds,
        SUM(CASE WHEN is_problem THEN 1 ELSE 0 END) AS problem_rows,
        ROUND(100.0 * SUM(CASE WHEN is_problem THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
            AS pct_problem_rows,
        AVG(pace_gap) AS avg_pace_gap_sec,
        APPROX_PERCENTILE(pace_gap, 0.5) AS median_pace_gap_sec
    FROM bucketed
    GROUP BY course_id, day_type, time_bucket, hour_of_day
)

SELECT
    *
FROM agg
ORDER BY course_id, day_type, time_bucket, hour_of_day


