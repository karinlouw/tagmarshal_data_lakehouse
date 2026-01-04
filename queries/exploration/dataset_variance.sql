-- Dataset Variance Analysis
-- Compares data volume and patterns across courses

SELECT 
    course_id,
    COUNT(*) as total_events,
    COUNT(DISTINCT round_id) as total_rounds,
    ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT round_id), 1) as avg_events_per_round,
    MIN(fix_timestamp) as earliest_data,
    MAX(fix_timestamp) as latest_data,
    COUNT(DISTINCT DATE(fix_timestamp)) as unique_days,
    MAX(location_index) as max_location_index,
    COUNT(DISTINCT hole_number) as unique_holes_seen,
    COUNT(DISTINCT start_hole) as unique_start_holes
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY total_events DESC

