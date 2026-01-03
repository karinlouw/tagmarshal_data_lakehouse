-- Critical Column Gaps Analysis
-- Identifies which courses have the most severe data quality issues
-- Prioritizes columns by their impact on analytics capabilities

{{ config(materialized='table') }}

WITH column_analysis AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT round_id) AS total_rounds,
        
        -- TIER 1: Critical for pace management (primary use case)
        SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS t1_null_pace,
        SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS t1_null_pace_gap,
        SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) AS t1_null_positional_gap,
        
        -- TIER 2: Critical for location tracking
        SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS t2_null_hole,
        SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS t2_null_section,
        SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) AS t2_null_lat,
        SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END) AS t2_null_lon,
        SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS t2_null_timestamp,
        
        -- TIER 3: Important for device/signal quality
        SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS t3_null_battery,
        SUM(CASE WHEN is_cache IS NULL THEN 1 ELSE 0 END) AS t3_null_cache,
        SUM(CASE WHEN is_projected IS NULL THEN 1 ELSE 0 END) AS t3_null_projected,
        SUM(CASE WHEN is_problem IS NULL THEN 1 ELSE 0 END) AS t3_null_problem,
        
        -- TIER 4: Round configuration metadata
        SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS t4_null_start_hole,
        SUM(CASE WHEN is_nine_hole IS NULL THEN 1 ELSE 0 END) AS t4_null_is_nine_hole,
        SUM(CASE WHEN is_complete IS NULL THEN 1 ELSE 0 END) AS t4_null_is_complete,
        SUM(CASE WHEN current_nine IS NULL THEN 1 ELSE 0 END) AS t4_null_current_nine,
        SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) AS t4_null_goal_time
        
    FROM {{ source('silver', 'fact_telemetry_event') }}
    GROUP BY course_id
)

SELECT
    course_id,
    total_events,
    total_rounds,
    
    -- TIER 1: Pace Management (Weight: 40%)
    -- These columns are ESSENTIAL for the primary use case
    ROUND(100.0 * t1_null_pace / total_events, 2) AS pct_null_pace,
    ROUND(100.0 * t1_null_pace_gap / total_events, 2) AS pct_null_pace_gap,
    ROUND(100.0 * t1_null_positional_gap / total_events, 2) AS pct_null_positional_gap,
    
    -- Tier 1 Impact Assessment
    CASE 
        WHEN 100.0 * t1_null_pace / total_events > 50 THEN '🔴 CRITICAL: Pace analysis NOT possible'
        WHEN 100.0 * t1_null_pace / total_events > 20 THEN '🟠 WARNING: Pace analysis degraded'
        WHEN 100.0 * t1_null_pace / total_events > 5 THEN '🟡 MINOR: Some pace gaps'
        ELSE '🟢 GOOD: Pace data complete'
    END AS pace_data_status,
    
    -- TIER 2: Location Tracking (Weight: 30%)
    ROUND(100.0 * t2_null_hole / total_events, 2) AS pct_null_hole,
    ROUND(100.0 * t2_null_section / total_events, 2) AS pct_null_section,
    ROUND(100.0 * t2_null_lat / total_events, 2) AS pct_null_latitude,
    ROUND(100.0 * t2_null_timestamp / total_events, 2) AS pct_null_timestamp,
    
    -- Tier 2 Impact Assessment
    CASE 
        WHEN 100.0 * t2_null_hole / total_events > 30 THEN '🔴 CRITICAL: Hole tracking broken'
        WHEN 100.0 * t2_null_hole / total_events > 10 THEN '🟠 WARNING: Location gaps detected'
        ELSE '🟢 GOOD: Location data complete'
    END AS location_data_status,
    
    -- TIER 3: Device Health (Weight: 20%)
    ROUND(100.0 * t3_null_battery / total_events, 2) AS pct_null_battery,
    ROUND(100.0 * t3_null_cache / total_events, 2) AS pct_null_cache_flag,
    
    -- Tier 3 Impact Assessment
    CASE 
        WHEN 100.0 * t3_null_battery / total_events > 50 THEN '🟠 WARNING: Cannot monitor device health'
        WHEN 100.0 * t3_null_battery / total_events > 20 THEN '🟡 MINOR: Some battery data missing'
        ELSE '🟢 GOOD: Device health trackable'
    END AS device_health_status,
    
    -- TIER 4: Round Configuration (Weight: 10%)
    ROUND(100.0 * t4_null_start_hole / total_events, 2) AS pct_null_start_hole,
    ROUND(100.0 * t4_null_is_nine_hole / total_events, 2) AS pct_null_nine_hole_flag,
    ROUND(100.0 * t4_null_goal_time / total_events, 2) AS pct_null_goal_time,
    
    -- Tier 4 Impact Assessment
    CASE 
        WHEN 100.0 * t4_null_goal_time / total_events > 80 THEN '🟠 WARNING: Goal times not set'
        WHEN 100.0 * t4_null_start_hole / total_events > 50 THEN '🟡 MINOR: Start hole unknown'
        ELSE '🟢 GOOD: Round config available'
    END AS round_config_status,
    
    -- OVERALL USABILITY SCORE
    -- Weighted: Tier1=40%, Tier2=30%, Tier3=20%, Tier4=10%
    ROUND(100 - (
        0.40 * (100.0 * (t1_null_pace + t1_null_pace_gap) / (2 * total_events)) +
        0.30 * (100.0 * (t2_null_hole + t2_null_timestamp) / (2 * total_events)) +
        0.20 * (100.0 * t3_null_battery / total_events) +
        0.10 * (100.0 * t4_null_goal_time / total_events)
    ), 1) AS usability_score,
    
    -- Actionable Recommendations
    CASE 
        WHEN 100.0 * t1_null_pace / total_events > 20 
        THEN 'Check pace calculation algorithm - many events missing pace values'
        WHEN 100.0 * t2_null_hole / total_events > 20 
        THEN 'Review location assignment logic - many events without hole numbers'
        WHEN 100.0 * t3_null_battery / total_events > 50 
        THEN 'Enable battery reporting on devices'
        WHEN 100.0 * t4_null_goal_time / total_events > 80 
        THEN 'Configure goal times for this course in the system'
        ELSE 'Data quality acceptable - monitor for changes'
    END AS top_recommendation

FROM column_analysis
ORDER BY usability_score ASC

