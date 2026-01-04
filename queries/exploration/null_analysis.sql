-- Null Value Analysis
-- Detailed breakdown of missing data across all critical columns
-- Updated to include timestamp null analysis using is_timestamp_missing flag

SELECT 
    course_id,
    COUNT(*) as total_rows,
    -- Timestamp (critical - now tracked with flag)
    SUM(CASE WHEN is_timestamp_missing = true THEN 1 ELSE 0 END) as null_timestamp,
    ROUND(100.0 * SUM(CASE WHEN is_timestamp_missing = true THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_timestamp,
    -- Pace columns
    SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) as null_pace,
    ROUND(100.0 * SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_pace,
    SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) as null_pace_gap,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_pace_gap,
    SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) as null_positional_gap,
    ROUND(100.0 * SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_positional_gap,
    -- Location columns
    SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) as null_hole,
    ROUND(100.0 * SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_hole,
    SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) as null_section,
    ROUND(100.0 * SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_section,
    SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) as null_gps,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_gps,
    -- Device columns
    SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) as null_battery,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_battery,
    -- Round config columns
    SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) as null_start_hole,
    ROUND(100.0 * SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_start_hole,
    SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) as null_goal_time,
    ROUND(100.0 * SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_goal_time
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY pct_null_timestamp DESC, course_id

