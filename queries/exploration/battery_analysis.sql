-- Battery Health Analysis by Course
-- Identifies courses with low battery device issues

SELECT 
    course_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) as low_battery,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_low_battery
FROM iceberg.silver.fact_telemetry_event
WHERE battery_percentage IS NOT NULL
GROUP BY course_id
ORDER BY pct_low_battery DESC

