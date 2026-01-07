-- gold_coverage_audit.sql
-- ---------------------------------------------------------------------------
-- Purpose:
--   A single per-course audit view that makes it obvious what Silver contains
--   and what each key Gold model includes. This is intentionally redundant and
--   "count-heavy" to support demos + data validation.
--
-- Notes:
--   - Silver counts include padding rows unless otherwise specified.
--   - Gold models typically exclude padding rows (by design).
--   - Seasonality models bucket missing timestamps into "Unknown" categories.
-- ---------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH courses AS (
    SELECT DISTINCT
        course_id
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE course_id IS NOT NULL
),

silver_event_counts AS (
    SELECT
        course_id,
        COUNT(*) AS silver_total_rows,
        SUM(CASE WHEN is_location_padding THEN 1 ELSE 0 END) AS silver_padding_rows,
        SUM(CASE WHEN NOT is_location_padding THEN 1 ELSE 0 END) AS silver_non_padding_rows,
        SUM(CASE WHEN is_timestamp_missing THEN 1 ELSE 0 END) AS silver_ts_missing_rows,
        SUM(
            CASE
                WHEN is_timestamp_missing AND NOT is_location_padding THEN 1
                ELSE 0
            END
        ) AS silver_ts_missing_non_padding_rows,
        COUNT(DISTINCT round_id) AS silver_distinct_rounds_all,
        COUNT(DISTINCT CASE WHEN NOT is_location_padding THEN round_id END) AS silver_distinct_rounds_non_padding
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE course_id IS NOT NULL
    GROUP BY course_id
),

gold_fact_rounds AS (
    SELECT
        course_id,
        COUNT(*) AS gold_fact_rounds_rows,
        COUNT(DISTINCT round_id) AS gold_fact_rounds_distinct_rounds,
        SUM(fix_count) AS gold_fact_rounds_sum_fix_count
    FROM {{ ref('fact_rounds') }}
    GROUP BY course_id
),

gold_hole_perf AS (
    SELECT
        course_id,
        COUNT(*) AS gold_hole_perf_rows,
        COUNT(DISTINCT round_id) AS gold_hole_perf_distinct_rounds,
        COUNT(DISTINCT CAST(round_id AS VARCHAR) || ':' || CAST(hole_number AS VARCHAR) || ':' || CAST(nine_number AS VARCHAR))
            AS gold_hole_perf_distinct_round_hole_nine
    FROM {{ ref('fact_round_hole_performance') }}
    GROUP BY course_id
),

gold_seasonality_month AS (
    SELECT
        course_id,
        SUM(rounds) AS gold_rounds_by_month_sum_rounds,
        SUM(CASE WHEN month_number = 0 THEN rounds ELSE 0 END) AS gold_rounds_by_month_unknown_ts_rounds
    FROM {{ ref('course_rounds_by_month') }}
    GROUP BY course_id
),

gold_seasonality_weekday AS (
    SELECT
        course_id,
        SUM(rounds) AS gold_rounds_by_weekday_sum_rounds,
        SUM(CASE WHEN weekday_number = 0 THEN rounds ELSE 0 END) AS gold_rounds_by_weekday_unknown_ts_rounds
    FROM {{ ref('course_rounds_by_weekday') }}
    GROUP BY course_id
),

gold_dim_course AS (
    SELECT
        course_id,
        1 AS gold_dim_course_present,
        unit_count
    FROM {{ ref('dim_course') }}
)

SELECT
    c.course_id,

    -- Silver telemetry counts
    s.silver_total_rows,
    s.silver_padding_rows,
    s.silver_non_padding_rows,
    s.silver_ts_missing_rows,
    s.silver_ts_missing_non_padding_rows,
    s.silver_distinct_rounds_all,
    s.silver_distinct_rounds_non_padding,

    -- Gold summary tables
    fr.gold_fact_rounds_rows,
    fr.gold_fact_rounds_distinct_rounds,
    fr.gold_fact_rounds_sum_fix_count,

    hp.gold_hole_perf_rows,
    hp.gold_hole_perf_distinct_rounds,
    hp.gold_hole_perf_distinct_round_hole_nine,

    -- Seasonality integrity (now includes unknown timestamp bucket)
    sm.gold_rounds_by_month_sum_rounds,
    sm.gold_rounds_by_month_unknown_ts_rounds,
    sw.gold_rounds_by_weekday_sum_rounds,
    sw.gold_rounds_by_weekday_unknown_ts_rounds,

    -- Presence checks / enrichment coverage
    dc.gold_dim_course_present,
    dc.unit_count

FROM courses c
LEFT JOIN silver_event_counts s ON c.course_id = s.course_id
LEFT JOIN gold_fact_rounds fr ON c.course_id = fr.course_id
LEFT JOIN gold_hole_perf hp ON c.course_id = hp.course_id
LEFT JOIN gold_seasonality_month sm ON c.course_id = sm.course_id
LEFT JOIN gold_seasonality_weekday sw ON c.course_id = sw.course_id
LEFT JOIN gold_dim_course dc ON c.course_id = dc.course_id
ORDER BY c.course_id


