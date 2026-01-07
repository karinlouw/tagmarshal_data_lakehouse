-- telemetry_completeness_summary.sql
-- ---------------------------------------------------------------------------
-- Purpose:
--   Provide a simple, fast Gold-layer view of how many Silver rows are:
--     - CSV padding slots (is_location_padding = TRUE)
--     - Missing timestamps (is_timestamp_missing = TRUE)
--   for each course. This is meant for completeness audits and demos, not
--   for filtering or excluding data.
-- ---------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        course_id,
        is_location_padding,
        is_timestamp_missing
    FROM {{ source('silver', 'fact_telemetry_event') }}
),

per_course AS (
    SELECT
        course_id,
        COUNT(*) AS total_rows,

        -- Padding vs non-padding rows
        SUM(CASE WHEN is_location_padding THEN 1 ELSE 0 END) AS padding_rows,
        SUM(CASE WHEN NOT is_location_padding THEN 1 ELSE 0 END) AS non_padding_rows,

        -- Timestamp-missing rows (including padding)
        SUM(CASE WHEN is_timestamp_missing THEN 1 ELSE 0 END) AS ts_missing_rows,

        -- Timestamp-missing rows on real telemetry events only (non-padding)
        SUM(
            CASE
                WHEN is_timestamp_missing AND NOT is_location_padding THEN 1
                ELSE 0
            END
        ) AS ts_missing_non_padding_rows
    FROM base
    GROUP BY course_id
)

SELECT
    course_id,
    total_rows,
    padding_rows,
    non_padding_rows,
    ts_missing_rows,
    ts_missing_non_padding_rows,

    -- Percentages (0–100) for quick comparison
    ROUND(100.0 * padding_rows / NULLIF(total_rows, 0), 2) AS pct_padding_total,
    ROUND(100.0 * ts_missing_rows / NULLIF(total_rows, 0), 2) AS pct_ts_missing_total,
    ROUND(
        100.0 * ts_missing_non_padding_rows / NULLIF(non_padding_rows, 0),
        2
    ) AS pct_ts_missing_non_padding
FROM per_course
ORDER BY course_id


