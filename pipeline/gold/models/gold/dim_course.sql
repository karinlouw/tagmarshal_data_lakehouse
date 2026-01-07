-- dim_course.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per course_id
-- Purpose:
--   A single "course info" dimension that stitches together:
--     - configuration (9/18/27, shotgun starts)
--     - data quality
--     - telemetry completeness (padding / missing timestamps)
--     - observed time range and playable modes (from fact_rounds)
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

WITH base_courses AS (
    -- Anchor the dimension on *all* observed courses in Silver, including
    -- padding-only courses. This prevents accidental drop of a course just
    -- because certain downstream models filter to non-padding rows.
    SELECT DISTINCT
        course_id
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE course_id IS NOT NULL
),

round_range AS (
    SELECT
        course_id,
        COUNT(*) AS rounds_observed,
        SUM(CASE WHEN is_complete THEN 1 ELSE 0 END) AS complete_rounds_observed,
        MIN(round_start_ts) AS first_round_start_ts,
        MAX(round_start_ts) AS last_round_start_ts,
        ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(round_length))) AS playable_round_lengths
    FROM {{ ref('fact_rounds') }}
    GROUP BY course_id
),

units AS (
    SELECT
        facility_id AS course_id,
        ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(unit_name) FILTER (WHERE unit_name IS NOT NULL))) AS unit_names
    FROM {{ source('silver', 'dim_facility_topology') }}
    GROUP BY facility_id
)

SELECT
    bc.course_id,

    -- Configuration / capacity
    c.likely_course_type,
    c.max_section_seen,
    c.max_holes_in_round,
    c.unique_start_holes,
    c.pct_shotgun_starts,
    c.pct_nine_hole,
    c.pct_full_rounds,
    c.course_complexity_score,

    -- Round range and playable modes
    r.rounds_observed,
    r.complete_rounds_observed,
    r.first_round_start_ts,
    r.last_round_start_ts,
    r.playable_round_lengths,

    -- Data quality (course-level)
    dq.data_quality_score,
    dq.overall_quality_score,
    dq.pct_missing_pace,
    dq.pct_missing_pace_gap,
    dq.pct_missing_hole_number,
    dq.pct_missing_section_number,
    dq.pct_missing_fix_timestamp,
    dq.pct_missing_start_hole,

    -- Telemetry completeness (padding / timestamps)
    tc.total_rows,
    tc.padding_rows,
    tc.non_padding_rows,
    tc.pct_padding_total,
    tc.pct_ts_missing_total,
    tc.pct_ts_missing_non_padding,

    -- Topology / naming (optional enrichment)
    u.unit_names,
    CASE
        WHEN u.unit_names IS NULL THEN 0
        ELSE CARDINALITY(u.unit_names)
    END AS unit_count

FROM base_courses bc
LEFT JOIN {{ ref('course_configuration_analysis') }} c
    ON bc.course_id = c.course_id
LEFT JOIN {{ ref('data_quality_overview') }} dq
    ON bc.course_id = dq.course_id
LEFT JOIN {{ ref('telemetry_completeness_summary') }} tc
    ON bc.course_id = tc.course_id
LEFT JOIN round_range r
    ON bc.course_id = r.course_id
LEFT JOIN units u
    ON bc.course_id = u.course_id


