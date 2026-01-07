-- Data Quality Overview by Course
-- Identifies missing values in critical columns for each course
-- Tiers: 1 (Pace), 2 (Location), 3 (Device), 4 (Config)

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        course_id,
        round_id,
        -- Tier 1: Pace
        pace,
        pace_gap,
        positional_gap,
        goal_time,
        
        -- Tier 2: Location
        latitude,
        longitude,
        fix_timestamp,
        hole_number,
        section_number,
        hole_section,
        nine_number,
        current_nine,
        
        -- Tier 3: Device
        battery_percentage,
        is_cache,
        is_projected,
        is_problem,
        is_timestamp_missing,
        
        -- Tier 4: Config
        start_hole,
        start_section,
        end_section,
        is_nine_hole,
        is_complete
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE is_location_padding = FALSE
),

course_stats AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT round_id) AS total_rounds,
        
        -- Tier 1: Pace (Null Counts)
        SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS null_pace,
        SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS null_pace_gap,
        SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) AS null_positional_gap,
        SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) AS null_goal_time,
        
        -- Tier 2: Location (Null Counts)
        SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS null_coordinates,
        SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS null_fix_timestamp,
        SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS null_hole_number,
        SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS null_section_number,
        SUM(CASE WHEN hole_section IS NULL THEN 1 ELSE 0 END) AS null_hole_section,
        SUM(CASE WHEN nine_number IS NULL THEN 1 ELSE 0 END) AS null_nine_number,
        SUM(CASE WHEN current_nine IS NULL THEN 1 ELSE 0 END) AS null_current_nine,
        
        -- Tier 3: Device (Null Counts)
        SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS null_battery,
        SUM(CASE WHEN is_cache IS NULL THEN 1 ELSE 0 END) AS null_is_cache,
        SUM(CASE WHEN is_projected IS NULL THEN 1 ELSE 0 END) AS null_is_projected,
        SUM(CASE WHEN is_problem IS NULL THEN 1 ELSE 0 END) AS null_is_problem,
        SUM(CASE WHEN is_timestamp_missing = TRUE THEN 1 ELSE 0 END) AS timestamp_missing_flag,

        -- Tier 4: Config (Null Counts)
        SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS null_start_hole,
        SUM(CASE WHEN start_section IS NULL THEN 1 ELSE 0 END) AS null_start_section,
        SUM(CASE WHEN end_section IS NULL THEN 1 ELSE 0 END) AS null_end_section,
        SUM(CASE WHEN is_nine_hole IS NULL THEN 1 ELSE 0 END) AS null_is_nine_hole,
        SUM(CASE WHEN is_complete IS NULL THEN 1 ELSE 0 END) AS null_is_complete,
        
        -- Specific Value Checks (for reference)
        SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) AS low_battery_events,
        SUM(CASE WHEN is_problem = TRUE THEN 1 ELSE 0 END) AS problem_events
    FROM base
    GROUP BY course_id
)

,
final AS (
SELECT
    course_id,
    total_events,
    total_rounds,
    
    -- Tier 1: Pace (% Missing)
    ROUND(100.0 * null_pace / NULLIF(total_events, 0), 2) AS pct_missing_pace,
    ROUND(100.0 * null_pace_gap / NULLIF(total_events, 0), 2) AS pct_missing_pace_gap,
    ROUND(100.0 * null_positional_gap / NULLIF(total_events, 0), 2) AS pct_missing_positional_gap,
    ROUND(100.0 * null_goal_time / NULLIF(total_events, 0), 2) AS pct_missing_goal_time,
    
    -- Tier 2: Location (% Missing)
    ROUND(100.0 * null_coordinates / NULLIF(total_events, 0), 2) AS pct_missing_coordinates,
    ROUND(100.0 * null_fix_timestamp / NULLIF(total_events, 0), 2) AS pct_missing_fix_timestamp,
    ROUND(100.0 * null_hole_number / NULLIF(total_events, 0), 2) AS pct_missing_hole_number,
    ROUND(100.0 * null_section_number / NULLIF(total_events, 0), 2) AS pct_missing_section_number,
    ROUND(100.0 * null_hole_section / NULLIF(total_events, 0), 2) AS pct_missing_hole_section,
    ROUND(100.0 * null_nine_number / NULLIF(total_events, 0), 2) AS pct_missing_nine_number,
    ROUND(100.0 * null_current_nine / NULLIF(total_events, 0), 2) AS pct_missing_current_nine,
    
    -- Tier 3: Device (% Missing)
    ROUND(100.0 * null_battery / NULLIF(total_events, 0), 2) AS pct_missing_battery,
    ROUND(100.0 * null_is_cache / NULLIF(total_events, 0), 2) AS pct_missing_is_cache,
    ROUND(100.0 * null_is_projected / NULLIF(total_events, 0), 2) AS pct_missing_is_projected,
    ROUND(100.0 * null_is_problem / NULLIF(total_events, 0), 2) AS pct_missing_is_problem,
    ROUND(100.0 * timestamp_missing_flag / NULLIF(total_events, 0), 2) AS pct_timestamp_missing_flag,
    
    -- Tier 4: Config (% Missing)
    ROUND(100.0 * null_start_hole / NULLIF(total_events, 0), 2) AS pct_missing_start_hole,
    ROUND(100.0 * null_start_section / NULLIF(total_events, 0), 2) AS pct_missing_start_section,
    ROUND(100.0 * null_end_section / NULLIF(total_events, 0), 2) AS pct_missing_end_section,
    ROUND(100.0 * null_is_nine_hole / NULLIF(total_events, 0), 2) AS pct_missing_is_nine_hole,
    ROUND(100.0 * null_is_complete / NULLIF(total_events, 0), 2) AS pct_missing_is_complete,
    
    -- Scores per Tier (100 - Average Missing %)
    -- Tier 1 Score (Pace)
    ROUND(100 - (
        (COALESCE(100.0 * null_pace / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_pace_gap / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_positional_gap / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_goal_time / NULLIF(total_events, 0), 0)) / 4
    ), 1) AS score_tier_1_pace,
    
    -- Tier 2 Score (Location)
    ROUND(100 - (
        (COALESCE(100.0 * null_coordinates / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_fix_timestamp / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_hole_number / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_section_number / NULLIF(total_events, 0), 0)) / 4
    ), 1) AS score_tier_2_location,
    
    -- Tier 3 Score (Device)
    ROUND(100 - (
        (COALESCE(100.0 * null_battery / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_is_cache / NULLIF(total_events, 0), 0) + 
         COALESCE(100.0 * timestamp_missing_flag / NULLIF(total_events, 0), 0)) / 3
    ), 1) AS score_tier_3_device,
    
    -- Tier 4 Score (Config)
    ROUND(100 - (
        (COALESCE(100.0 * null_start_hole / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_is_nine_hole / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_is_complete / NULLIF(total_events, 0), 0)) / 3
    ), 1) AS score_tier_4_config,
    
    -- Overall Score (Average of Tier Scores)
    ROUND(
        (
          (100 - ((COALESCE(100.0 * null_pace / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_pace_gap / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_positional_gap / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_goal_time / NULLIF(total_events, 0), 0)) / 4)) +
          (100 - ((COALESCE(100.0 * null_coordinates / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_fix_timestamp / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_hole_number / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_section_number / NULLIF(total_events, 0), 0)) / 4)) +
          (100 - ((COALESCE(100.0 * null_battery / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_is_cache / NULLIF(total_events, 0), 0) + COALESCE(100.0 * timestamp_missing_flag / NULLIF(total_events, 0), 0)) / 3)) +
          (100 - ((COALESCE(100.0 * null_start_hole / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_is_nine_hole / NULLIF(total_events, 0), 0) + COALESCE(100.0 * null_is_complete / NULLIF(total_events, 0), 0)) / 3))
        ) / 4
    , 1) AS data_quality_score

FROM course_stats
)
SELECT
  final.*,
  -- Backward-compatible alias (older dashboards/queries may reference this name)
  data_quality_score AS overall_quality_score
FROM final
ORDER BY data_quality_score ASC
