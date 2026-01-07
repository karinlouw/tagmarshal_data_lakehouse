-- fact_rounds.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per (course_id, round_id)
-- Purpose:
--   Canonical "rounds" fact table for exploration and downstream joins.
--   Combines round configuration + basic pace + basic data quality signals.
-- -----------------------------------------------------------------------------

{{ config(
    materialized='table',
    partition_by=['course_id']
) }}

WITH base AS (
    SELECT
        course_id,
        round_id,
        fix_timestamp,
        is_timestamp_missing,
        hole_number,
        section_number,
        nine_number,
        hole_section,
        pace,
        pace_gap,
        positional_gap,
        battery_percentage,
        is_projected,
        is_problem,
        is_cache,
        -- Round configuration (round-level fields repeated on each fix)
        start_hole,
        start_section,
        end_section,
        is_nine_hole,
        is_complete
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE is_location_padding = FALSE
),

round_rollup AS (
    SELECT
        course_id,
        round_id,

        -- Timing / volume
        MIN(fix_timestamp) AS round_start_ts,
        MAX(fix_timestamp) AS round_end_ts,
        DATE_DIFF('second', MIN(fix_timestamp), MAX(fix_timestamp)) AS duration_sec,
        COUNT(*) AS fix_count,

        -- "Completeness" signals
        SUM(CASE WHEN is_timestamp_missing THEN 1 ELSE 0 END) AS ts_missing_fix_count,
        COUNT(DISTINCT hole_number) AS holes_played,
        COUNT(DISTINCT nine_number) AS nines_played,
        MIN(section_number) AS min_section_number,
        MAX(section_number) AS max_section_number,
        MIN(CASE WHEN hole_section = 1 THEN section_number END) AS first_tee_section_number,

        -- Round configuration
        MAX(start_hole) AS start_hole,
        MAX(start_section) AS start_section,
        MAX(end_section) AS end_section,
        BOOL_OR(COALESCE(is_nine_hole, FALSE)) AS is_nine_hole,
        BOOL_OR(COALESCE(is_complete, FALSE)) AS is_complete,

        -- Pace (round averages across fixes)
        AVG(pace) AS avg_pace_sec,
        MAX(pace) AS max_pace_sec,
        AVG(pace_gap) AS avg_pace_gap_sec,
        AVG(positional_gap) AS avg_positional_gap,

        -- Signal quality
        SUM(CASE WHEN is_projected THEN 1 ELSE 0 END) AS projected_fix_count,
        SUM(CASE WHEN is_problem THEN 1 ELSE 0 END) AS problem_fix_count,

        -- Device health
        MIN(battery_percentage) AS min_battery_pct,
        AVG(battery_percentage) AS avg_battery_pct,
        SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) AS low_battery_fix_count,
        SUM(CASE WHEN battery_percentage < 10 THEN 1 ELSE 0 END) AS critical_battery_fix_count,

        -- Caching (often indicates offline / delayed transmission)
        SUM(CASE WHEN is_cache THEN 1 ELSE 0 END) AS cached_fix_count
    FROM base
    GROUP BY course_id, round_id
)

SELECT
    r.*,

    -- Round duration in minutes (convenience column for analysis)
    ROUND(CAST(duration_sec AS DOUBLE) / 60.0, 1) AS round_duration_minutes,

    -- Rates for quick filtering / dashboards
    CAST(projected_fix_count AS DOUBLE) / NULLIF(fix_count, 0) AS projected_rate,
    CAST(problem_fix_count AS DOUBLE) / NULLIF(fix_count, 0) AS problem_rate,
    CAST(cached_fix_count AS DOUBLE) / NULLIF(fix_count, 0) AS cached_rate,
    CAST(ts_missing_fix_count AS DOUBLE) / NULLIF(fix_count, 0) AS ts_missing_rate,

    -- Round "length" classification (useful for 9/18/27 understanding)
    CASE
        WHEN nines_played >= 3 OR max_section_number > 54 THEN '27'
        WHEN nines_played = 2 OR max_section_number > 27 THEN '18'
        WHEN nines_played = 1 THEN '9'
        ELSE 'unknown'
    END AS round_length,

    -- Convenience date and date parts for analysis
    CAST(round_start_ts AS DATE) AS round_date,
    YEAR(round_start_ts) AS round_year,
    MONTH(round_start_ts) AS round_month,
    DAY(round_start_ts) AS round_day,
    DAY_OF_WEEK(round_start_ts) AS round_weekday  -- 1=Mon, 7=Sun in Trino
FROM round_rollup r


