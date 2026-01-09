{{ config(
    materialized='table',
    post_hook=tm_iceberg_partitioning_course_id_post_hook()
) }}

WITH round_starts AS (
    -- Use fact_rounds to avoid a second scan of fix-grain telemetry.
    SELECT
        course_id,
        round_id,
        round_start_ts AS round_start_time
    FROM {{ ref('fact_rounds') }}
),
rounds_with_month_keys AS (
    SELECT
        course_id,
        round_id,
        -- If a round has no timestamps at all, keep it and bucket it explicitly.
        CASE
            WHEN round_start_time IS NULL THEN DATE '1900-01-01'
            ELSE DATE_TRUNC('month', round_start_time)
        END AS month_start,
        CASE
            WHEN round_start_time IS NULL THEN 0
            ELSE EXTRACT(MONTH FROM round_start_time)
        END AS month_number,
        CASE
            WHEN round_start_time IS NULL THEN 'Unknown (missing timestamp)'
            ELSE FORMAT_DATETIME(round_start_time, 'MMMM')
        END AS month_name
    FROM round_starts
),
monthly_rounds AS (
    SELECT
        course_id,
        month_start,
        month_number,
        month_name,
        COUNT(DISTINCT round_id) AS rounds
    FROM rounds_with_month_keys
    GROUP BY
        course_id,
        month_start,
        month_number,
        month_name
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
