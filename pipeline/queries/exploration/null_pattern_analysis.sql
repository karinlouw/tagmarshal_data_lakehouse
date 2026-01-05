-- ============================================================================
-- NULL PATTERN ANALYSIS QUERIES
-- ============================================================================
-- These queries help analyze missing timestamps and other null patterns
-- in the Silver layer fact_telemetry_event table.
--
-- Usage:
--   just trino-query "SELECT * FROM (...)" 
--   Or copy queries into Trino shell: just trino
-- ============================================================================

-- ============================================================================
-- 1. OVERVIEW: Count of missing timestamps
-- ============================================================================
-- Get overall percentage of rows with missing timestamps
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE is_timestamp_missing = true) as missing_timestamp_count,
    COUNT(*) FILTER (WHERE is_timestamp_missing = false) as valid_timestamp_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_timestamp_missing = true) / COUNT(*), 2) as missing_percentage
FROM iceberg.silver.fact_telemetry_event;


-- ============================================================================
-- 2. BY COURSE: Which courses have the most missing timestamps?
-- ============================================================================
SELECT 
    course_id,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE is_timestamp_missing = true) as missing_count,
    COUNT(*) FILTER (WHERE is_timestamp_missing = false) as valid_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_timestamp_missing = true) / COUNT(*), 2) as missing_percentage
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY missing_percentage DESC, missing_count DESC;


-- ============================================================================
-- 3. BY INGEST DATE: Are missing timestamps more common in certain dates?
-- ============================================================================
SELECT 
    ingest_date,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE is_timestamp_missing = true) as missing_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_timestamp_missing = true) / COUNT(*), 2) as missing_percentage
FROM iceberg.silver.fact_telemetry_event
GROUP BY ingest_date
ORDER BY ingest_date DESC;


-- ============================================================================
-- 4. BY ROUND: Which rounds have missing timestamps?
-- ============================================================================
SELECT 
    course_id,
    round_id,
    COUNT(*) as total_fixes,
    COUNT(*) FILTER (WHERE is_timestamp_missing = true) as missing_timestamp_count,
    COUNT(*) FILTER (WHERE is_timestamp_missing = false) as valid_timestamp_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_timestamp_missing = true) / COUNT(*), 2) as missing_percentage
FROM iceberg.silver.fact_telemetry_event
WHERE is_timestamp_missing = true
GROUP BY course_id, round_id
ORDER BY missing_count DESC
LIMIT 20;


-- ============================================================================
-- 5. PATTERN ANALYSIS: Are missing timestamps clustered in location_index?
-- ============================================================================
-- Check if missing timestamps occur at specific positions in the location array
SELECT 
    location_index,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE is_timestamp_missing = true) as missing_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_timestamp_missing = true) / COUNT(*), 2) as missing_percentage
FROM iceberg.silver.fact_telemetry_event
GROUP BY location_index
HAVING COUNT(*) FILTER (WHERE is_timestamp_missing = true) > 0
ORDER BY location_index;


-- ============================================================================
-- 6. CORRELATION: Do missing timestamps correlate with other null fields?
-- ============================================================================
-- Check if rows with missing timestamps also have other null values
SELECT 
    is_timestamp_missing,
    COUNT(*) as row_count,
    COUNT(*) FILTER (WHERE longitude IS NULL) as null_longitude_count,
    COUNT(*) FILTER (WHERE latitude IS NULL) as null_latitude_count,
    COUNT(*) FILTER (WHERE pace IS NULL) as null_pace_count,
    COUNT(*) FILTER (WHERE battery_percentage IS NULL) as null_battery_count,
    COUNT(*) FILTER (WHERE hole_number IS NULL) as null_hole_count
FROM iceberg.silver.fact_telemetry_event
GROUP BY is_timestamp_missing
ORDER BY is_timestamp_missing;


-- ============================================================================
-- 7. SAMPLE: View actual rows with missing timestamps
-- ============================================================================
-- See what the data looks like when timestamps are missing
SELECT 
    course_id,
    round_id,
    location_index,
    hole_number,
    section_number,
    longitude,
    latitude,
    pace,
    is_cache,
    is_timestamp_missing,
    fix_timestamp,
    event_date
FROM iceberg.silver.fact_telemetry_event
WHERE is_timestamp_missing = true
ORDER BY course_id, round_id, location_index
LIMIT 50;


-- ============================================================================
-- 8. FILTER: Query only rows WITH valid timestamps (for normal analysis)
-- ============================================================================
-- Use this filter when you need to exclude missing timestamps
SELECT 
    course_id,
    round_id,
    fix_timestamp,
    hole_number,
    latitude,
    longitude,
    pace
FROM iceberg.silver.fact_telemetry_event
WHERE is_timestamp_missing = false  -- Only rows with valid timestamps
    AND course_id = 'americanfalls'
ORDER BY fix_timestamp
LIMIT 100;


-- ============================================================================
-- 9. DATA QUALITY REPORT: Comprehensive null analysis
-- ============================================================================
-- Generate a comprehensive report of all null patterns
SELECT 
    course_id,
    COUNT(*) as total_rows,
    -- Timestamp nulls
    COUNT(*) FILTER (WHERE is_timestamp_missing = true) as missing_timestamps,
    -- Coordinate nulls
    COUNT(*) FILTER (WHERE longitude IS NULL OR latitude IS NULL) as missing_coordinates,
    -- Pace nulls
    COUNT(*) FILTER (WHERE pace IS NULL) as missing_pace,
    -- Battery nulls
    COUNT(*) FILTER (WHERE battery_percentage IS NULL) as missing_battery,
    -- Hole/section nulls
    COUNT(*) FILTER (WHERE hole_number IS NULL) as missing_hole,
    COUNT(*) FILTER (WHERE section_number IS NULL) as missing_section,
    -- Rows with multiple nulls
    COUNT(*) FILTER (
        WHERE is_timestamp_missing = true 
        AND (longitude IS NULL OR latitude IS NULL)
    ) as missing_timestamp_and_coords
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY missing_timestamps DESC;


-- ============================================================================
-- 10. TREND ANALYSIS: Missing timestamps over time
-- ============================================================================
-- Analyze if missing timestamps are increasing/decreasing over time
SELECT 
    DATE_TRUNC('month', ingest_date) as ingest_month,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE is_timestamp_missing = true) as missing_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_timestamp_missing = true) / COUNT(*), 2) as missing_percentage
FROM iceberg.silver.fact_telemetry_event
GROUP BY DATE_TRUNC('month', ingest_date)
ORDER BY ingest_month DESC;

