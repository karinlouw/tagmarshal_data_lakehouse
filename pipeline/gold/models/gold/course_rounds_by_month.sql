{{ config(
    materialized='table',
    partition_by=['course_id']
) }}

WITH round_starts AS (
    SELECT
        course_id,
        round_id,
        MIN(fix_timestamp) AS round_start_time
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE fix_timestamp IS NOT NULL
    GROUP BY course_id, round_id
),
monthly_rounds AS (
    SELECT
        course_id,
        DATE_TRUNC('month', round_start_time) AS month_start,
        EXTRACT(MONTH FROM round_start_time) AS month_number,
        FORMAT_DATETIME(round_start_time, 'MMMM') AS month_name,
        COUNT(DISTINCT round_id) AS rounds
    FROM round_starts
    GROUP BY
        course_id,
        DATE_TRUNC('month', round_start_time),
        EXTRACT(MONTH FROM round_start_time),
        FORMAT_DATETIME(round_start_time, 'MMMM')
),
total_rounds_per_course AS (
    SELECT
        course_id,
        SUM(rounds) AS total_rounds
    FROM monthly_rounds
    GROUP BY course_id
)
SELECT
    mr.course_id,
    mr.month_start,
    mr.month_number,
    mr.month_name,
    mr.rounds,
    ROUND(CAST(mr.rounds AS DOUBLE) / tr.total_rounds * 100, 2) AS pct_total
FROM monthly_rounds mr
JOIN total_rounds_per_course tr
    ON mr.course_id = tr.course_id
ORDER BY mr.course_id, mr.month_start
