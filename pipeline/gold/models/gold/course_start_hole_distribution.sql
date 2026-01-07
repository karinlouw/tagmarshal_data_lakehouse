-- course_start_hole_distribution.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per (course_id, start_hole)
-- Purpose:
--   Shotgun start / start hole distribution per course with sanity checks
--   based on section numbers.
-- -----------------------------------------------------------------------------

{{ config(
    materialized='table',
    partition_by=['course_id']
) }}

WITH rounds AS (
    SELECT
        course_id,
        round_id,
        start_hole,
        start_section,
        min_section_number,
        first_tee_section_number,
        is_complete
    FROM {{ ref('fact_rounds') }}
    WHERE start_hole IS NOT NULL
),

start_stats AS (
    SELECT
        course_id,
        start_hole,
        COUNT(*) AS rounds_with_this_start,
        SUM(CASE WHEN is_complete THEN 1 ELSE 0 END) AS complete_rounds_with_this_start,

        -- Sanity: does the telemetry "first section" line up with configured start_section?
        MIN(min_section_number) AS min_min_section_number,
        MAX(min_section_number) AS max_min_section_number,
        MIN(start_section) AS min_start_section,
        MAX(start_section) AS max_start_section,
        MIN(first_tee_section_number) AS min_first_tee_section_number,
        MAX(first_tee_section_number) AS max_first_tee_section_number
    FROM rounds
    GROUP BY course_id, start_hole
),

course_totals AS (
    SELECT
        course_id,
        SUM(rounds_with_this_start) AS total_rounds,
        SUM(complete_rounds_with_this_start) AS total_complete_rounds,
        COUNT(DISTINCT start_hole) AS distinct_start_holes
    FROM start_stats
    GROUP BY course_id
)

SELECT
    s.course_id,
    s.start_hole,
    s.rounds_with_this_start,
    s.complete_rounds_with_this_start,
    t.total_rounds,
    t.total_complete_rounds,
    t.distinct_start_holes,
    ROUND(100.0 * s.rounds_with_this_start / NULLIF(t.total_rounds, 0), 2) AS pct_rounds_with_this_start,
    ROUND(100.0 * s.complete_rounds_with_this_start / NULLIF(t.total_complete_rounds, 0), 2) AS pct_complete_rounds_with_this_start,
    s.min_min_section_number,
    s.max_min_section_number,
    s.min_start_section,
    s.max_start_section,
    s.min_first_tee_section_number,
    s.max_first_tee_section_number
FROM start_stats s
JOIN course_totals t
    ON s.course_id = t.course_id


