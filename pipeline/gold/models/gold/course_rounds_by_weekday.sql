{{ config(materialized='table') }}

-- Course seasonality (weekday)
-- Counts rounds by the weekday of the round start timestamp.

WITH round_starts AS (
    SELECT
        course_id,
        round_id,
        MIN(fix_timestamp) AS round_start_time
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE fix_timestamp IS NOT NULL
    GROUP BY course_id, round_id
),
weekday AS (
    SELECT
        course_id,
        day_of_week(round_start_time) AS weekday_number,
        format_datetime(round_start_time, 'EEEE') AS weekday_name,
        COUNT(*) AS rounds
    FROM round_starts
    GROUP BY course_id, day_of_week(round_start_time), format_datetime(round_start_time, 'EEEE')
)
SELECT *
FROM weekday
ORDER BY course_id, weekday_number


