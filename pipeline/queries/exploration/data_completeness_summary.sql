-- Data Completeness Summary
-- Overview of data quality for exploration and audit phase
-- Shows total records, null counts, and completeness percentages

SELECT 
    course_id,
    COUNT(*) as total_records,
    COUNT(DISTINCT round_id) as unique_rounds,
    
    -- Critical columns completeness
    ROUND(100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL AND is_timestamp_missing = false THEN 1 ELSE 0 END) / COUNT(*), 1) as timestamp_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_gap_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN positional_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pos_gap_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as hole_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as gps_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as battery_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN start_hole IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as start_hole_complete_pct,
    
    -- Overall completeness score (average of key columns)
    ROUND(
        (
            100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL AND is_timestamp_missing = false THEN 1 ELSE 0 END) / COUNT(*) +
            100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) +
            100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) +
            100.0 * SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*)
        ) / 4, 1
    ) as overall_completeness_score

FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY overall_completeness_score DESC

