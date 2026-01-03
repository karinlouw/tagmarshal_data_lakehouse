-- Data Quality Overview by Course
-- Identifies missing values in critical columns for each course
-- Use this to assess data reliability and completeness

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        course_id,
        round_id,
        -- Critical fields for pace analysis
        pace,
        pace_gap,
        hole_number,
        section_number,
        nine_number,
        -- Critical fields for location tracking
        latitude,
        longitude,
        fix_timestamp,
        -- Critical fields for device health
        battery_percentage,
        is_cache,
        is_projected,
        is_problem,
        -- Round configuration (important for course complexity)
        start_hole,
        is_nine_hole,
        is_complete
    FROM {{ source('silver', 'fact_telemetry_event') }}
),

course_stats AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT round_id) AS total_rounds,
        
        -- Pace analysis completeness
        SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS null_pace,
        SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS null_pace_gap,
        
        -- Location completeness
        SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS null_hole_number,
        SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS null_section_number,
        SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS null_coordinates,
        SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
        
        -- Device health completeness
        SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS null_battery,
        SUM(CASE WHEN is_cache IS NULL THEN 1 ELSE 0 END) AS null_is_cache,
        
        -- Round configuration completeness
        SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS null_start_hole,
        SUM(CASE WHEN is_nine_hole IS NULL THEN 1 ELSE 0 END) AS null_is_nine_hole,
        SUM(CASE WHEN is_complete IS NULL THEN 1 ELSE 0 END) AS null_is_complete,
        
        -- Problem indicators
        SUM(CASE WHEN is_problem = TRUE THEN 1 ELSE 0 END) AS problem_events,
        SUM(CASE WHEN is_projected = TRUE THEN 1 ELSE 0 END) AS projected_events,
        SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) AS low_battery_events
    FROM base
    GROUP BY course_id
)

SELECT
    course_id,
    total_events,
    total_rounds,
    
    -- Percentages for critical fields (pace analysis)
    ROUND(100.0 * null_pace / total_events, 2) AS pct_missing_pace,
    ROUND(100.0 * null_pace_gap / total_events, 2) AS pct_missing_pace_gap,
    
    -- Percentages for location fields
    ROUND(100.0 * null_hole_number / total_events, 2) AS pct_missing_hole,
    ROUND(100.0 * null_section_number / total_events, 2) AS pct_missing_section,
    ROUND(100.0 * null_coordinates / total_events, 2) AS pct_missing_coords,
    ROUND(100.0 * null_timestamp / total_events, 2) AS pct_missing_timestamp,
    
    -- Percentages for device health
    ROUND(100.0 * null_battery / total_events, 2) AS pct_missing_battery,
    ROUND(100.0 * null_is_cache / total_events, 2) AS pct_missing_cache_flag,
    
    -- Percentages for round configuration
    ROUND(100.0 * null_start_hole / total_events, 2) AS pct_missing_start_hole,
    ROUND(100.0 * null_is_nine_hole / total_events, 2) AS pct_missing_nine_hole_flag,
    ROUND(100.0 * null_is_complete / total_events, 2) AS pct_missing_complete_flag,
    
    -- Problem indicators as percentages
    ROUND(100.0 * problem_events / total_events, 2) AS pct_problem_events,
    ROUND(100.0 * projected_events / total_events, 2) AS pct_projected_events,
    ROUND(100.0 * low_battery_events / total_events, 2) AS pct_low_battery,
    
    -- Overall data quality score (higher = better)
    -- Weighted average: pace fields (40%), location fields (30%), device health (30%)
    ROUND(100 - (
        0.4 * (COALESCE(100.0 * null_pace / NULLIF(total_events, 0), 0) + 
               COALESCE(100.0 * null_pace_gap / NULLIF(total_events, 0), 0)) / 2 +
        0.3 * (COALESCE(100.0 * null_hole_number / NULLIF(total_events, 0), 0) + 
               COALESCE(100.0 * null_coordinates / NULLIF(total_events, 0), 0)) / 2 +
        0.3 * (COALESCE(100.0 * null_battery / NULLIF(total_events, 0), 0) + 
               COALESCE(100.0 * low_battery_events / NULLIF(total_events, 0), 0)) / 2
    ), 1) AS data_quality_score

FROM course_stats
ORDER BY data_quality_score ASC

