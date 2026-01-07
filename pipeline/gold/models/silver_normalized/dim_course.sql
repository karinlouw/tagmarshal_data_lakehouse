-- dim_course (Silver-normalized)
-- -----------------------------------------------------------------------------
-- Grain: one row per course_id
-- Purpose:
--   Normalized course dimension for exploration. This anchors on *observed*
--   course_ids in Silver telemetry, and optionally enriches with course_profile.
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH observed AS (
    SELECT DISTINCT
        course_id
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE course_id IS NOT NULL
),
profile AS (
    SELECT
        course_id,
        course_type,
        volume_profile,
        peak_season_start_month,
        peak_season_end_month,
        notes,
        source AS profile_source
    FROM {{ source('silver', 'dim_course_profile') }}
)
SELECT
    o.course_id,
    p.course_type,
    p.volume_profile,
    p.peak_season_start_month,
    p.peak_season_end_month,
    p.notes,
    p.profile_source
FROM observed o
LEFT JOIN profile p
    ON o.course_id = p.course_id
ORDER BY o.course_id


