"""SQL queries for the data quality dashboard.

Each query includes a description explaining what it does.
Queries are organised by dashboard page.
"""

# =============================================================================
# Executive / Data quality page queries
# =============================================================================

EXECUTIVE_SUMMARY = """
-- Executive summary: total courses, rounds, and events
SELECT 
    COUNT(DISTINCT course_id) as total_courses,
    COUNT(DISTINCT round_id) as total_rounds,
    COUNT(*) as total_events
FROM iceberg.silver.fact_telemetry_event
"""

DATA_QUALITY_OVERVIEW = """
-- Data quality overview by course
-- Shows completeness percentages and overall quality score
SELECT
    course_id,
    total_events,
    total_rounds,
    pct_missing_pace,
    pct_missing_pace_gap,
    pct_missing_hole,
    pct_missing_section,
    pct_missing_coords,
    pct_missing_timestamp,
    pct_missing_battery,
    pct_missing_cache_flag,
    pct_missing_start_hole,
    pct_missing_nine_hole_flag,
    pct_missing_complete_flag,
    pct_problem_events,
    pct_projected_events,
    pct_low_battery,
    data_quality_score
FROM iceberg.gold.data_quality_overview
ORDER BY data_quality_score DESC
"""

CRITICAL_COLUMN_GAPS = """
-- Critical column gaps by tier
-- Tier 1: Pace (essential), Tier 2: Location, Tier 3: Device, Tier 4: Config
SELECT
    course_id,
    total_events,
    total_rounds,
    pct_null_pace,
    pct_null_pace_gap,
    pct_null_positional_gap,
    pace_data_status,
    pct_null_hole,
    pct_null_section,
    pct_null_latitude,
    pct_null_timestamp,
    location_data_status,
    pct_null_battery,
    pct_null_cache_flag,
    device_health_status,
    pct_null_start_hole,
    pct_null_nine_hole_flag,
    pct_null_goal_time,
    round_config_status,
    usability_score,
    top_recommendation
FROM iceberg.gold.critical_column_gaps
ORDER BY usability_score ASC
"""

COLUMN_COMPLETENESS = """
-- Column completeness analysis for heatmap
SELECT 
    course_id,
    COUNT(*) as total,
    ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_pct,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pace_gap_pct,
    ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as hole_pct,
    ROUND(100.0 * SUM(CASE WHEN section_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as section_pct,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as gps_pct,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as battery_pct,
    ROUND(100.0 * SUM(CASE WHEN start_hole IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as start_hole_pct,
    ROUND(100.0 * SUM(CASE WHEN goal_time IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as goal_time_pct,
    ROUND(100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as timestamp_pct,
    ROUND(100.0 * SUM(CASE WHEN is_complete IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as is_complete_pct
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY course_id
"""

# =============================================================================
# Course configuration page queries
# =============================================================================

COURSE_CONFIGURATION = """
-- Course configuration analysis
-- Shows course types, round completion, and complexity
SELECT
    course_id,
    total_rounds,
    likely_course_type,
    max_section_seen,
    max_holes_in_round,
    pct_complete,
    pct_incomplete,
    pct_nine_hole,
    pct_full_rounds,
    unique_start_holes,
    pct_shotgun_starts,
    pct_single_nine,
    pct_two_nines,
    pct_all_three_nines,
    avg_locations_per_round,
    min_locations_per_round,
    max_locations_per_round,
    course_complexity_score
FROM iceberg.gold.course_configuration_analysis
ORDER BY course_complexity_score DESC
"""

# =============================================================================
# Course topology page queries
# =============================================================================

FACILITY_TOPOLOGY = """
-- Facility topology configuration
-- Maps course sections to physical units (nines)
SELECT
    facility_id,
    unit_id,
    unit_name,
    nine_number,
    section_start,
    section_end
FROM iceberg.silver.dim_facility_topology
ORDER BY facility_id, nine_number
"""

TOPOLOGY_VALIDATION_BRADSHAW = """
-- Bradshaw topology validation: events by unit
SELECT
    nine_number,
    COUNT(DISTINCT round_id) AS rounds,
    COUNT(*) AS fixes,
    ROUND(AVG(pace), 1) AS avg_pace_sec,
    ROUND(AVG(pace_gap), 1) AS avg_pace_gap_sec
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'bradshawfarmgc'
  AND nine_number IS NOT NULL
GROUP BY nine_number
ORDER BY nine_number
"""

LOOP_FATIGUE_AMERICAN_FALLS = """
-- American Falls loop fatigue analysis
-- Compares pace on same hole across loop 1 vs loop 2
SELECT 
    hole_number,
    nine_number,
    ROUND(AVG(avg_pace_sec), 1) as avg_pace_seconds,
    COUNT(*) as sample_size
FROM iceberg.gold.fact_round_hole_performance
WHERE course_id = 'americanfalls'
GROUP BY hole_number, nine_number
ORDER BY hole_number, nine_number
"""

SHOTGUN_START_DISTRIBUTION = """
-- Indian Creek shotgun start distribution
SELECT
    start_hole,
    COUNT(DISTINCT round_id) AS rounds
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'indiancreek'
  AND start_hole IS NOT NULL
GROUP BY start_hole
ORDER BY rounds DESC, start_hole
"""

# =============================================================================
# Seasonality page queries
# =============================================================================

ROUNDS_BY_MONTH = """
-- Rounds by month with percentage of total
SELECT
    course_id,
    month_start,
    month_number,
    month_name,
    rounds,
    pct_total
FROM iceberg.gold.course_rounds_by_month
ORDER BY course_id, month_start
"""

ROUNDS_BY_WEEKDAY = """
-- Rounds by weekday
SELECT
    course_id,
    weekday_number,
    weekday_name,
    rounds
FROM iceberg.gold.course_rounds_by_weekday
ORDER BY course_id, weekday_number
"""

# =============================================================================
# Device health and pace analysis page queries
# =============================================================================

SIGNAL_QUALITY = """
-- Signal quality by round
SELECT
    course_id,
    round_id,
    fix_count,
    projected_fix_count,
    problem_fix_count,
    projected_rate,
    problem_rate
FROM iceberg.gold.signal_quality_rounds
"""

SIGNAL_QUALITY_SUMMARY = """
-- Signal quality summary by course
SELECT
    course_id,
    COUNT(*) as total_rounds,
    SUM(fix_count) as total_fixes,
    SUM(projected_fix_count) as total_projected,
    SUM(problem_fix_count) as total_problems,
    ROUND(100.0 * SUM(projected_fix_count) / NULLIF(SUM(fix_count), 0), 2) as pct_projected,
    ROUND(100.0 * SUM(problem_fix_count) / NULLIF(SUM(fix_count), 0), 2) as pct_problems
FROM iceberg.gold.signal_quality_rounds
GROUP BY course_id
ORDER BY pct_problems DESC
"""

BATTERY_HEALTH = """
-- Battery health analysis by course
SELECT 
    course_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) as low_battery_events,
    SUM(CASE WHEN battery_percentage < 10 THEN 1 ELSE 0 END) as critical_battery_events,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_low_battery,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage < 10 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_critical_battery
FROM iceberg.silver.fact_telemetry_event
WHERE battery_percentage IS NOT NULL
GROUP BY course_id
ORDER BY pct_low_battery DESC
"""

DEVICE_HEALTH_ERRORS = """
-- Device health error events
SELECT
    course_id,
    round_id,
    fix_timestamp,
    battery_percentage,
    health_flag
FROM iceberg.gold.device_health_errors
ORDER BY course_id, fix_timestamp
"""


def get_bottleneck_query(course_id: str) -> str:
    """Generate bottleneck analysis query for a specific course.

    Args:
        course_id: Course identifier to analyse

    Returns:
        SQL query string
    """
    return f"""
-- Bottleneck analysis by section for {course_id}
-- Identifies where groups bunch up using pace_gap
SELECT 
    course_id,
    hole_number,
    section_number,
    hole_section,
    ROUND(AVG(latitude), 6) as lat,
    ROUND(AVG(longitude), 6) as lon,
    ROUND(AVG(pace_gap), 0) as avg_pace_gap_seconds,
    ROUND(STDDEV(pace_gap), 0) as pace_gap_stddev,
    ROUND(AVG(positional_gap), 0) as avg_positional_gap,
    ROUND(AVG(pace), 0) as avg_pace_seconds,
    COUNT(DISTINCT round_id) as rounds_measured,
    COUNT(*) as total_fixes
FROM iceberg.silver.fact_telemetry_event 
WHERE course_id = '{course_id}'
  AND latitude IS NOT NULL 
  AND longitude IS NOT NULL 
  AND pace_gap IS NOT NULL
  AND hole_number IS NOT NULL
GROUP BY course_id, hole_number, section_number, hole_section
HAVING COUNT(*) > 20
ORDER BY section_number
"""


PACE_SUMMARY = """
-- Pace summary by round
SELECT
    course_id,
    round_id,
    round_start_ts,
    round_end_ts,
    fix_count,
    avg_pace,
    avg_pace_gap,
    avg_positional_gap
FROM iceberg.gold.pace_summary_by_round
"""

PACE_BY_HOLE = """
-- Pace by hole performance
SELECT
    course_id,
    round_id,
    hole_number,
    nine_number,
    course_unit,
    duration_sec,
    avg_pace_sec,
    max_pace_sec,
    avg_pace_gap_sec
FROM iceberg.gold.fact_round_hole_performance
ORDER BY course_id, round_id, nine_number, hole_number
"""

# =============================================================================
# Course list query (for filters)
# =============================================================================

COURSE_LIST = """
-- List of all courses
SELECT DISTINCT course_id
FROM iceberg.silver.fact_telemetry_event
ORDER BY course_id
"""
