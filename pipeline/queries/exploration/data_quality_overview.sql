-- Data Quality Overview by Course
-- Shows data quality scores and key metrics per course

SELECT 
    course_id,
    total_events,
    total_rounds,
    ROUND(data_quality_score, 1) as data_quality_score,
    ROUND(pct_missing_pace, 1) as pct_missing_pace,
    ROUND(pct_missing_hole, 1) as pct_missing_hole,
    ROUND(pct_low_battery, 1) as pct_low_battery
FROM iceberg.gold.data_quality_overview
ORDER BY data_quality_score DESC

