-- Executive Summary Query
-- Returns total courses, rounds, and events across all data

SELECT 
    COUNT(DISTINCT course_id) as total_courses,
    COUNT(DISTINCT round_id) as total_rounds,
    COUNT(*) as total_events
FROM iceberg.silver.fact_telemetry_event

