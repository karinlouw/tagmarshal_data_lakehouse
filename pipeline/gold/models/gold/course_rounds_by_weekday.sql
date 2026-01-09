{{ config(materialized='table') }}

-- Course seasonality (weekday)
-- Counts rounds by the weekday of the round start timestamp.
-- Uses `gold.fact_rounds` to avoid re-scanning fix-grain telemetry.

WITH round_starts AS (
    SELECT
        course_id,
        round_id,
        round_start_ts AS round_start_time
    FROM {{ ref('fact_rounds') }}
),
weekday AS (
    SELECT
        course_id,
        CASE
            WHEN round_start_time IS NULL THEN 0
            ELSE day_of_week(round_start_time)
        END AS weekday_number,
        CASE
            WHEN round_start_time IS NULL THEN 'Unknown (missing timestamp)'
            ELSE format_datetime(round_start_time, 'EEEE')
        END AS weekday_name,
        COUNT(*) AS rounds
    FROM round_starts
    GROUP BY
        course_id,
        CASE
            WHEN round_start_time IS NULL THEN 0
            ELSE day_of_week(round_start_time)
        END,
        CASE
            WHEN round_start_time IS NULL THEN 'Unknown (missing timestamp)'
            ELSE format_datetime(round_start_time, 'EEEE')
        END
)
SELECT *
FROM weekday
ORDER BY course_id, weekday_number


