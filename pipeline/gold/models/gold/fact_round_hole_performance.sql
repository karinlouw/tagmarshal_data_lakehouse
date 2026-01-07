-- pipeline/gold/models/gold/fact_round_hole_performance.sql

{{ config(
    materialized='table',
    partition_by=['course_id']
) }}

WITH hole_stats AS (
    SELECT
        course_id,
        round_id,
        hole_number,
        nine_number,
        
        -- Critical for differentiating Loop 1 vs Loop 2 (e.g. American Falls)
        -- We group by nine_number to separate the first 9 (nine_number=1) from second 9 (nine_number=2)
        -- even if they play the same physical hole_number (e.g. Hole 5).
        
        -- Timing
        MIN(fix_timestamp) AS hole_start_time,
        MAX(fix_timestamp) AS hole_end_time,
        DATE_DIFF('second', MIN(fix_timestamp), MAX(fix_timestamp)) AS duration_sec,
        
        -- Pace Metrics (Avg of all fixes in the hole)
        AVG(pace) AS avg_pace_sec,
        MAX(pace) AS max_pace_sec,
        AVG(pace_gap) AS avg_pace_gap_sec,
        
        -- Completion Status
        BOOL_OR(is_complete) AS round_was_completed
        
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE is_location_padding = FALSE
      AND hole_number IS NOT NULL
    GROUP BY course_id, round_id, hole_number, nine_number
    ORDER BY round_id, hole_number, nine_number
)

SELECT
    h.course_id,
    h.round_id,
    h.hole_number,
    h.nine_number,
    t.unit_name AS course_unit,
    h.hole_start_time,
    h.hole_end_time,
    h.duration_sec,
    h.avg_pace_sec,
    h.max_pace_sec,
    h.avg_pace_gap_sec,
    h.round_was_completed
FROM hole_stats h
LEFT JOIN {{ source('silver', 'dim_facility_topology') }} t
    ON h.course_id = t.facility_id 
    AND h.nine_number = t.nine_number

