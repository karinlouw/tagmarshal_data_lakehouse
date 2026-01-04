-- Pace Gap Coverage Analysis
-- Shows missing pace_gap values by course

SELECT 
    course_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) as missing_pace_gap,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_missing
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY pct_missing DESC

