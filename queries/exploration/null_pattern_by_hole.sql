-- Null Pattern Analysis by Hole
-- Identifies which holes have the most missing data
-- Useful for understanding if certain holes have data collection issues

SELECT 
    course_id,
    hole_number,
    COUNT(*) as total_records,
    
    -- Null counts
    SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) as null_pace,
    SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) as null_pace_gap,
    SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) as null_pos_gap,
    SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) as null_battery,
    SUM(CASE WHEN is_timestamp_missing = true THEN 1 ELSE 0 END) as null_timestamp,
    
    -- Null percentages
    ROUND(100.0 * SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_null_pace,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_null_pace_gap,
    
    -- Overall null rate for this hole
    ROUND(
        100.0 * (
            SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) +
            SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) +
            SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END)
        ) / (COUNT(*) * 3), 1
    ) as avg_null_rate

FROM iceberg.silver.fact_telemetry_event
WHERE hole_number IS NOT NULL
GROUP BY course_id, hole_number
ORDER BY course_id, hole_number

