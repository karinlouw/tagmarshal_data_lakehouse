-- Common SQL Queries for TagMarshal Data Lakehouse
-- Example queries for common data exploration tasks

-- ============================================
-- BASIC COUNTS
-- ============================================

-- Count rounds by course
SELECT 
    course_id, 
    COUNT(DISTINCT round_id) as rounds
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY rounds DESC;

-- Count events by course
SELECT 
    course_id,
    COUNT(*) as total_events
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY total_events DESC;

-- ============================================
-- PACE ANALYSIS
-- ============================================

-- Average pace by hole for a specific course
SELECT 
    hole_number,
    AVG(pace) as avg_pace,
    COUNT(*) as fixes
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'indiancreek'
  AND hole_number IS NOT NULL
GROUP BY hole_number
ORDER BY hole_number;

-- Find slow rounds (high average pace)
SELECT 
    round_id,
    course_id,
    AVG(pace) as avg_pace,
    COUNT(*) as event_count
FROM iceberg.silver.fact_telemetry_event
WHERE pace IS NOT NULL
GROUP BY round_id, course_id
HAVING AVG(pace) > 500  -- More than 500 seconds behind goal
ORDER BY avg_pace DESC
LIMIT 10;

-- ============================================
-- TIME-BASED ANALYSIS
-- ============================================

-- Events by hour of day
SELECT 
    HOUR(fix_timestamp) as hour_of_day,
    COUNT(*) as fixes,
    COUNT(DISTINCT round_id) as rounds
FROM iceberg.silver.fact_telemetry_event
WHERE fix_timestamp IS NOT NULL
GROUP BY HOUR(fix_timestamp)
ORDER BY hour_of_day;

-- Events by date
SELECT 
    DATE(fix_timestamp) as event_date,
    course_id,
    COUNT(*) as events,
    COUNT(DISTINCT round_id) as rounds
FROM iceberg.silver.fact_telemetry_event
WHERE fix_timestamp IS NOT NULL
GROUP BY DATE(fix_timestamp), course_id
ORDER BY event_date DESC, course_id;

-- ============================================
-- DATA QUALITY CHECKS
-- ============================================

-- Check for duplicate round_id + fix_timestamp combinations
SELECT 
    round_id,
    fix_timestamp,
    COUNT(*) as duplicate_count
FROM iceberg.silver.fact_telemetry_event
GROUP BY round_id, fix_timestamp
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Missing critical fields by course
SELECT 
    course_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) as missing_pace,
    SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) as missing_hole,
    SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) as missing_gps
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY course_id;

