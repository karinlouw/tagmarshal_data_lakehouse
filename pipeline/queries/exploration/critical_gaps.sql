-- Critical Column Gaps Analysis
-- Identifies courses with missing critical data and provides recommendations

SELECT 
    course_id,
    total_events,
    total_rounds,
    ROUND(usability_score, 1) as usability_score,
    pace_data_status,
    location_data_status,
    device_health_status,
    round_config_status,
    top_recommendation
FROM iceberg.gold.critical_column_gaps
ORDER BY usability_score DESC

