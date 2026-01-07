-- dim_device (Silver-normalized)
-- -----------------------------------------------------------------------------
-- Grain: one row per device_id
-- Purpose:
--   Normalized device dimension for exploration. Keeps the latest observed
--   battery stats and first/last seen timestamps (when available).
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        device AS device_id,
        course_id,
        fix_timestamp,
        is_timestamp_missing,
        battery_percentage
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE device IS NOT NULL
),
agg AS (
    SELECT
        device_id,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT course_id) AS courses_seen,
        MIN(fix_timestamp) AS first_seen_ts,
        MAX(fix_timestamp) AS last_seen_ts,
        SUM(CASE WHEN is_timestamp_missing THEN 1 ELSE 0 END) AS ts_missing_rows,
        MIN(battery_percentage) AS min_battery_pct,
        MAX(battery_percentage) AS max_battery_pct,
        AVG(battery_percentage) AS avg_battery_pct
    FROM base
    GROUP BY device_id
)
SELECT
    *
FROM agg
ORDER BY device_id


