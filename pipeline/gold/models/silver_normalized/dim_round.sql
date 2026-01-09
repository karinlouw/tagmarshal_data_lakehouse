-- dim_round (Silver-normalized)
-- -----------------------------------------------------------------------------
-- Grain: one row per (course_id, round_id)
-- Purpose:
--   Normalized round dimension built from Silver telemetry. This makes
--   exploration easier without forcing users to reason at fix granularity.
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        course_id,
        round_id,

        -- Fix-level fields (for rollups)
        fix_timestamp,
        is_timestamp_missing,
        is_location_padding,
        hole_number,
        section_number,
        nine_number,

        -- Round-level fields (repeated per fix)
        round_start_time,
        round_end_time,
        start_hole,
        start_section,
        end_section,
        is_nine_hole,
        current_nine,
        goal_time,
        goal_name,
        goal_time_fraction,
        is_complete,
        is_incomplete,
        is_secondary,
        is_auto_assigned,
        device,
        first_fix,
        last_fix
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE course_id IS NOT NULL
      AND round_id IS NOT NULL
),
round_rollup AS (
    SELECT
        course_id,
        round_id,

        -- Counts
        COUNT(*) AS total_rows,
        SUM(CASE WHEN is_location_padding THEN 1 ELSE 0 END) AS padding_rows,
        SUM(CASE WHEN NOT is_location_padding THEN 1 ELSE 0 END) AS non_padding_rows,
        SUM(CASE WHEN is_timestamp_missing THEN 1 ELSE 0 END) AS ts_missing_rows,
        SUM(
            CASE
                WHEN is_timestamp_missing AND NOT is_location_padding THEN 1
                ELSE 0
            END
        ) AS ts_missing_non_padding_rows,

        -- Timing (from fix timestamps)
        MIN(fix_timestamp) AS first_fix_ts,
        MAX(fix_timestamp) AS last_fix_ts,

        -- Coverage
        COUNT(DISTINCT hole_number) AS holes_observed,
        COUNT(DISTINCT nine_number) AS nines_observed,
        MIN(section_number) AS min_section_number,
        MAX(section_number) AS max_section_number,

        -- Stable round-level fields (choose a deterministic aggregation)
        MAX(round_start_time) AS round_start_time,
        MAX(round_end_time) AS round_end_time,
        MAX(start_hole) AS start_hole,
        MAX(start_section) AS start_section,
        MAX(end_section) AS end_section,
        BOOL_OR(COALESCE(is_nine_hole, FALSE)) AS is_nine_hole,
        BOOL_OR(COALESCE(is_complete, FALSE)) AS is_complete,
        BOOL_OR(COALESCE(is_incomplete, FALSE)) AS is_incomplete,
        BOOL_OR(COALESCE(is_secondary, FALSE)) AS is_secondary,
        BOOL_OR(COALESCE(is_auto_assigned, FALSE)) AS is_auto_assigned,
        MAX(current_nine) AS current_nine,
        MAX(goal_time) AS goal_time,
        MAX(goal_name) AS goal_name,
        MAX(goal_time_fraction) AS goal_time_fraction,
        MAX(device) AS device_id,
        MAX(first_fix) AS first_fix,
        MAX(last_fix) AS last_fix
    FROM base
    GROUP BY course_id, round_id
)
SELECT
    *
FROM round_rollup
ORDER BY course_id, round_id


