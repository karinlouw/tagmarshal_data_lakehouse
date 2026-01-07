-- ============================================================================
-- PURPOSE: Analyze data quality gaps by course
-- 
-- This query identifies which golf courses have the most severe data quality 
-- issues by counting missing (NULL) values in critical columns. It prioritizes
-- columns by their impact on analytics capabilities and provides actionable
-- recommendations.
-- ============================================================================

{{ config(materialized='table') }}

-- STEP 1: Count null values for each column, grouped by course
-- This CTE aggregates all the raw counts we need for analysis
WITH column_analysis AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT round_id) AS total_rounds,
        
        -- TIER 1: Most critical columns for pace management (primary use case)
        SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS t1_null_pace,
        SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS t1_null_pace_gap,
        SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) AS t1_null_positional_gap,
        
        -- TIER 2: Critical for location tracking
        SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS t2_null_hole,
        SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS t2_null_section,
        SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) AS t2_null_lat,
        SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS t2_null_timestamp,
        
        -- TIER 3: Important for device/signal quality monitoring
        SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS t3_null_battery,
        SUM(CASE WHEN is_projected IS NULL THEN 1 ELSE 0 END) AS t3_null_projected,
        
        -- TIER 4: Round configuration metadata
        SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS t4_null_start_hole,
        SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) AS t4_null_goal_time
        
    FROM {{ source('silver', 'fact_telemetry_event') }}
    WHERE is_location_padding = FALSE
    GROUP BY course_id
),

-- STEP 2: Calculate percentages once to avoid repeating calculations
percentages AS (
    SELECT
        *,
        -- Calculate percentage of null values for each tier
        -- Tier 1: Pace data (most critical)
        100.0 * t1_null_pace / total_events AS pct_null_pace,
        100.0 * t1_null_pace_gap / total_events AS pct_null_pace_gap,
        100.0 * t1_null_positional_gap / total_events AS pct_null_positional_gap,
        GREATEST(
            100.0 * t1_null_pace / total_events,
            100.0 * t1_null_pace_gap / total_events
        ) AS pct_null_pace_worst,
        
        -- Tier 2: Location data
        100.0 * t2_null_hole / total_events AS pct_null_hole,
        100.0 * t2_null_section / total_events AS pct_null_section,
        100.0 * t2_null_lat / total_events AS pct_null_latitude,
        100.0 * t2_null_timestamp / total_events AS pct_null_timestamp,
        
        -- Tier 3: Device health (average of battery and projected flags)
        100.0 * t3_null_battery / total_events AS pct_null_battery,
        100.0 * (t3_null_battery + t3_null_projected) / (2 * total_events) AS pct_null_device_health,
        
        -- Tier 4: Round configuration (average of goal_time and start_hole)
        100.0 * t4_null_start_hole / total_events AS pct_null_start_hole,
        100.0 * t4_null_goal_time / total_events AS pct_null_goal_time,
        100.0 * (t4_null_goal_time + t4_null_start_hole) / (2 * total_events) AS pct_null_round_config
        
    FROM column_analysis
)

-- STEP 3: Final output with status assessments and recommendations
SELECT
    course_id,
    total_events,
    total_rounds,
    
    -- ========================================================================
    -- TIER 1: PACE MANAGEMENT DATA (Weight: 40% in usability score)
    -- These are the most critical columns - without them, pace analysis fails
    -- ========================================================================
    ROUND(pct_null_pace, 2) AS pct_null_pace,
    ROUND(pct_null_pace_gap, 2) AS pct_null_pace_gap,
    ROUND(pct_null_positional_gap, 2) AS pct_null_positional_gap,
    
    -- Status assessment: How bad is the pace data quality?
    CASE 
        WHEN pct_null_pace_worst > 50 THEN '🔴 CRITICAL: Pace analysis NOT possible'
        WHEN pct_null_pace_worst > 20 THEN '🟠 WARNING: Pace analysis degraded'
        WHEN pct_null_pace_worst > 5 THEN '🟡 MINOR: Some pace gaps'
        ELSE '🟢 GOOD: Pace data complete'
    END AS pace_data_status,
    
    -- ========================================================================
    -- TIER 2: LOCATION TRACKING DATA (Weight: 30% in usability score)
    -- Critical for knowing where golfers are on the course
    -- ========================================================================
    ROUND(pct_null_hole, 2) AS pct_null_hole,
    ROUND(pct_null_section, 2) AS pct_null_section,
    ROUND(pct_null_latitude, 2) AS pct_null_latitude,
    ROUND(pct_null_timestamp, 2) AS pct_null_timestamp,
    
    -- Status assessment: Can we track location properly?
    CASE 
        WHEN pct_null_hole > 30 THEN '🔴 CRITICAL: Hole tracking broken'
        WHEN pct_null_hole > 10 THEN '🟠 WARNING: Location gaps detected'
        ELSE '🟢 GOOD: Location data complete'
    END AS location_data_status,
    
    -- ========================================================================
    -- TIER 3: DEVICE HEALTH DATA (Weight: 20% in usability score)
    -- Important for monitoring device battery and signal quality
    -- ========================================================================
    ROUND(pct_null_battery, 2) AS pct_null_battery,
    
    -- Status assessment: Can we monitor device health?
    CASE 
        WHEN pct_null_device_health > 50 THEN '🟠 WARNING: Cannot monitor device health'
        WHEN pct_null_device_health > 20 THEN '🟡 MINOR: Some battery data missing'
        ELSE '🟢 GOOD: Device health trackable'
    END AS device_health_status,
    
    -- ========================================================================
    -- TIER 4: ROUND CONFIGURATION DATA (Weight: 10% in usability score)
    -- Metadata about round setup (less critical for core analytics)
    -- ========================================================================
    ROUND(pct_null_start_hole, 2) AS pct_null_start_hole,
    ROUND(pct_null_goal_time, 2) AS pct_null_goal_time,
    
    -- Status assessment: Is round configuration available?
    CASE 
        WHEN pct_null_round_config > 80 THEN '🟠 WARNING: Goal times not set'
        WHEN pct_null_round_config > 50 THEN '🟡 MINOR: Start hole unknown'
        ELSE '🟢 GOOD: Round config available'
    END AS round_config_status,
    
    -- ========================================================================
    -- OVERALL USABILITY SCORE
    -- Weighted average: Tier1=40%, Tier2=30%, Tier3=20%, Tier4=10%
    -- Higher score = better data quality (100 = perfect, 0 = unusable)
    -- ========================================================================
    ROUND(
        100 - (
            0.40 * pct_null_pace_worst +
            0.30 * (pct_null_hole + pct_null_timestamp) / 2 +
            0.20 * pct_null_device_health +
            0.10 * pct_null_round_config
        ),
        1
    ) AS usability_score,
    
    -- ========================================================================
    -- ACTIONABLE RECOMMENDATIONS
    -- Provides the most important fix to improve data quality for this course
    -- ========================================================================
    CASE 
        WHEN pct_null_pace_worst > 20 
            THEN 'Check pace calculation algorithm - many events missing pace values'
        WHEN pct_null_hole > 20 
            THEN 'Review location assignment logic - many events without hole numbers'
        WHEN pct_null_device_health > 50 
            THEN 'Enable battery reporting on devices'
        WHEN pct_null_round_config > 80 
            THEN 'Configure goal times for this course in the system'
        ELSE 'Data quality acceptable - monitor for changes'
    END AS top_recommendation

FROM percentages
-- Order by worst courses first (lowest usability score = most problems)
ORDER BY usability_score ASC

