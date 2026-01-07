"""SQL queries for the data quality dashboard.

Each query includes a description explaining what it does.
Queries are organised by dashboard page.
"""

# =============================================================================
# Executive / Data quality page queries
# =============================================================================

EXECUTIVE_SUMMARY = """
-- Executive summary: total courses, rounds, and events
-- - Courses + rounds come from the canonical Gold rounds fact (1 row per round)
-- - Events come from Silver non-padding telemetry (real fixes only)
WITH gold AS (
    SELECT
        COUNT(DISTINCT course_id) AS total_courses,
        COUNT(*) AS total_rounds
    FROM iceberg.gold.fact_rounds
),
silver AS (
SELECT 
        COUNT(*) AS total_events
FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
)
SELECT
    gold.total_courses,
    gold.total_rounds,
    silver.total_events
FROM gold
CROSS JOIN silver
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
    device_health_status,
    pct_null_start_hole,
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
# Column completeness (missing table) - generated directly by SQL for display
# =============================================================================

COLUMN_COMPLETENESS_MISSING_TABLE = """
-- Missing counts and missing % by course and column (for the dashboard table)
-- This is derived directly from silver, with all rounding done in SQL.
-- Excludes padding rows (real telemetry events only).
WITH base AS (
  SELECT
    course_id,
    COUNT(*) AS total_events,
    SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS missing_pace,
    SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS missing_pace_gap,
    SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS missing_hole,
    SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS missing_section,
    SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS missing_gps,
    SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS missing_battery,
    SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS missing_start_hole,
    SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) AS missing_goal_time,
    SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS missing_timestamp,
    SUM(CASE WHEN is_complete IS NULL THEN 1 ELSE 0 END) AS missing_is_complete
  FROM iceberg.silver.fact_telemetry_event
  WHERE is_location_padding = FALSE
  GROUP BY course_id
)
SELECT
  course_id AS "Course",
  missing_pace AS "Pace (count)",
  ROUND(100.0 * missing_pace / NULLIF(total_events, 0), 1) AS "Pace (%)",
  missing_pace_gap AS "Pace Gap (count)",
  ROUND(100.0 * missing_pace_gap / NULLIF(total_events, 0), 1) AS "Pace Gap (%)",
  missing_hole AS "Hole (count)",
  ROUND(100.0 * missing_hole / NULLIF(total_events, 0), 1) AS "Hole (%)",
  missing_section AS "Section (count)",
  ROUND(100.0 * missing_section / NULLIF(total_events, 0), 1) AS "Section (%)",
  missing_gps AS "GPS (count)",
  ROUND(100.0 * missing_gps / NULLIF(total_events, 0), 1) AS "GPS (%)",
  missing_battery AS "Battery (count)",
  ROUND(100.0 * missing_battery / NULLIF(total_events, 0), 1) AS "Battery (%)",
  missing_start_hole AS "Start hole (count)",
  ROUND(100.0 * missing_start_hole / NULLIF(total_events, 0), 1) AS "Start hole (%)",
  missing_goal_time AS "Goal time (count)",
  ROUND(100.0 * missing_goal_time / NULLIF(total_events, 0), 1) AS "Goal time (%)",
  missing_timestamp AS "Start time (count)",
  ROUND(100.0 * missing_timestamp / NULLIF(total_events, 0), 1) AS "Start time (%)",
  missing_is_complete AS "Is complete (count)",
  ROUND(100.0 * missing_is_complete / NULLIF(total_events, 0), 1) AS "Is complete (%)"
FROM base
ORDER BY "Course"
"""

COLUMN_COMPLETENESS_MISSING_TABLE_WITH_PADDING = """
-- Missing counts and missing % by course and column, broken down by padding vs non-padding
-- This shows how much of the missing data is from padding rows
WITH base AS (
  SELECT
    course_id,
    is_location_padding,
    COUNT(*) AS total_events,
    SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS missing_pace,
    SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS missing_pace_gap,
    SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS missing_hole,
    SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS missing_section,
    SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS missing_gps,
    SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS missing_battery,
    SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS missing_start_hole,
    SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) AS missing_goal_time,
    SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS missing_timestamp,
    SUM(CASE WHEN is_complete IS NULL THEN 1 ELSE 0 END) AS missing_is_complete
  FROM iceberg.silver.fact_telemetry_event
  GROUP BY course_id, is_location_padding
),
aggregated AS (
  SELECT
    course_id,
    SUM(CASE WHEN is_location_padding THEN total_events ELSE 0 END) AS padding_total,
    SUM(CASE WHEN NOT is_location_padding THEN total_events ELSE 0 END) AS non_padding_total,
    SUM(total_events) AS total_events,
    -- Missing from padding rows
    SUM(CASE WHEN is_location_padding THEN missing_pace ELSE 0 END) AS padding_missing_pace,
    SUM(CASE WHEN is_location_padding THEN missing_pace_gap ELSE 0 END) AS padding_missing_pace_gap,
    SUM(CASE WHEN is_location_padding THEN missing_hole ELSE 0 END) AS padding_missing_hole,
    SUM(CASE WHEN is_location_padding THEN missing_section ELSE 0 END) AS padding_missing_section,
    SUM(CASE WHEN is_location_padding THEN missing_gps ELSE 0 END) AS padding_missing_gps,
    SUM(CASE WHEN is_location_padding THEN missing_battery ELSE 0 END) AS padding_missing_battery,
    -- Missing from non-padding rows
    SUM(CASE WHEN NOT is_location_padding THEN missing_pace ELSE 0 END) AS non_padding_missing_pace,
    SUM(CASE WHEN NOT is_location_padding THEN missing_pace_gap ELSE 0 END) AS non_padding_missing_pace_gap,
    SUM(CASE WHEN NOT is_location_padding THEN missing_hole ELSE 0 END) AS non_padding_missing_hole,
    SUM(CASE WHEN NOT is_location_padding THEN missing_section ELSE 0 END) AS non_padding_missing_section,
    SUM(CASE WHEN NOT is_location_padding THEN missing_gps ELSE 0 END) AS non_padding_missing_gps,
    SUM(CASE WHEN NOT is_location_padding THEN missing_battery ELSE 0 END) AS non_padding_missing_battery,
    -- Total missing (all rows)
    SUM(missing_pace) AS total_missing_pace,
    SUM(missing_pace_gap) AS total_missing_pace_gap,
    SUM(missing_hole) AS total_missing_hole,
    SUM(missing_section) AS total_missing_section,
    SUM(missing_gps) AS total_missing_gps,
    SUM(missing_battery) AS total_missing_battery
  FROM base
  GROUP BY course_id
)
SELECT
  course_id AS "Course",
  padding_total AS "Padding rows",
  non_padding_total AS "Non-padding rows",
  total_events AS "Total rows",
  -- Show what % of missing data is from padding
  ROUND(100.0 * padding_missing_pace / NULLIF(total_missing_pace, 0), 1) AS "Pace missing from padding (%)",
  ROUND(100.0 * padding_missing_pace_gap / NULLIF(total_missing_pace_gap, 0), 1) AS "Pace Gap missing from padding (%)",
  ROUND(100.0 * padding_missing_hole / NULLIF(total_missing_hole, 0), 1) AS "Hole missing from padding (%)",
  ROUND(100.0 * padding_missing_section / NULLIF(total_missing_section, 0), 1) AS "Section missing from padding (%)",
  ROUND(100.0 * padding_missing_gps / NULLIF(total_missing_gps, 0), 1) AS "GPS missing from padding (%)",
  ROUND(100.0 * padding_missing_battery / NULLIF(total_missing_battery, 0), 1) AS "Battery missing from padding (%)",
  -- Completeness excluding padding
  ROUND(100.0 * (non_padding_total - non_padding_missing_pace) / NULLIF(non_padding_total, 0), 1) AS "Pace completeness (no padding)",
  ROUND(100.0 * (non_padding_total - non_padding_missing_pace_gap) / NULLIF(non_padding_total, 0), 1) AS "Pace Gap completeness (no padding)",
  ROUND(100.0 * (non_padding_total - non_padding_missing_hole) / NULLIF(non_padding_total, 0), 1) AS "Hole completeness (no padding)",
  ROUND(100.0 * (non_padding_total - non_padding_missing_section) / NULLIF(non_padding_total, 0), 1) AS "Section completeness (no padding)",
  ROUND(100.0 * (non_padding_total - non_padding_missing_gps) / NULLIF(non_padding_total, 0), 1) AS "GPS completeness (no padding)",
  ROUND(100.0 * (non_padding_total - non_padding_missing_battery) / NULLIF(non_padding_total, 0), 1) AS "Battery completeness (no padding)",
  -- Completeness including padding
  ROUND(100.0 * (total_events - total_missing_pace) / NULLIF(total_events, 0), 1) AS "Pace completeness (with padding)",
  ROUND(100.0 * (total_events - total_missing_pace_gap) / NULLIF(total_events, 0), 1) AS "Pace Gap completeness (with padding)",
  ROUND(100.0 * (total_events - total_missing_hole) / NULLIF(total_events, 0), 1) AS "Hole completeness (with padding)",
  ROUND(100.0 * (total_events - total_missing_section) / NULLIF(total_events, 0), 1) AS "Section completeness (with padding)",
  ROUND(100.0 * (total_events - total_missing_gps) / NULLIF(total_events, 0), 1) AS "GPS completeness (with padding)",
  ROUND(100.0 * (total_events - total_missing_battery) / NULLIF(total_events, 0), 1) AS "Battery completeness (with padding)"
FROM aggregated
ORDER BY "Course"
"""

COLUMN_COMPLETENESS_NON_PADDING = """
-- Column completeness analysis for heatmap (excluding padding rows)
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
WHERE is_location_padding = FALSE
GROUP BY course_id
ORDER BY course_id
"""

TELEMETRY_COMPLETENESS_SUMMARY = """
-- Get padding statistics from gold layer
SELECT
    course_id,
    total_rows,
    padding_rows,
    non_padding_rows,
    pct_padding_total,
    ts_missing_rows,
    ts_missing_non_padding_rows,
    pct_ts_missing_total,
    pct_ts_missing_non_padding
FROM iceberg.gold.telemetry_completeness_summary
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
  AND is_location_padding = FALSE
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
  AND is_location_padding = FALSE
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
-- Excludes padding rows (real telemetry events only)
SELECT 
    course_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) as low_battery_events,
    SUM(CASE WHEN battery_percentage < 10 THEN 1 ELSE 0 END) as critical_battery_events,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_low_battery,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage < 10 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_critical_battery
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND battery_percentage IS NOT NULL
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
  AND is_location_padding = FALSE
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
-- List of all courses (based on real telemetry only; excludes padding-only courses)
SELECT DISTINCT course_id
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
ORDER BY course_id
"""

# =============================================================================
# DBT Provenance SQL (silver → gold transformations)
# These show how gold tables are built from silver, for educational/demo purposes
# =============================================================================

DBT_DATA_QUALITY_OVERVIEW = """
-- dbt model: pipeline/gold/models/gold/data_quality_overview.sql
-- This is the transformation from silver -> gold
WITH base AS (
    SELECT
        course_id,
        round_id,
        pace,
        pace_gap,
        positional_gap,
        goal_time,
        latitude,
        longitude,
        fix_timestamp,
        hole_number,
        section_number,
        hole_section,
        nine_number,
        current_nine,
        battery_percentage,
        is_cache,
        is_projected,
        is_problem,
        is_timestamp_missing,
        start_hole,
        start_section,
        end_section,
        is_nine_hole,
        is_complete
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
),
course_stats AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT round_id) AS total_rounds,
        SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS null_pace,
        SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS null_pace_gap,
        SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) AS null_positional_gap,
        SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) AS null_goal_time,
        SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS null_coordinates,
        SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS null_fix_timestamp,
        SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS null_hole_number,
        SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS null_section_number,
        SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS null_battery,
        SUM(CASE WHEN is_cache IS NULL THEN 1 ELSE 0 END) AS null_is_cache,
        SUM(CASE WHEN is_timestamp_missing = TRUE THEN 1 ELSE 0 END) AS timestamp_missing_flag,
        SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS null_start_hole,
        SUM(CASE WHEN is_nine_hole IS NULL THEN 1 ELSE 0 END) AS null_is_nine_hole,
        SUM(CASE WHEN is_complete IS NULL THEN 1 ELSE 0 END) AS null_is_complete
    FROM base
    GROUP BY course_id
)
SELECT
    course_id,
    total_events,
    total_rounds,
    ROUND(100.0 * null_pace / NULLIF(total_events, 0), 2) AS pct_missing_pace,
    ROUND(100.0 * null_pace_gap / NULLIF(total_events, 0), 2) AS pct_missing_pace_gap,
    ROUND(100.0 * null_hole_number / NULLIF(total_events, 0), 2) AS pct_missing_hole,
    ROUND(100.0 * null_section_number / NULLIF(total_events, 0), 2) AS pct_missing_section,
    ROUND(100.0 * null_coordinates / NULLIF(total_events, 0), 2) AS pct_missing_coords,
    ROUND(100.0 * null_fix_timestamp / NULLIF(total_events, 0), 2) AS pct_missing_timestamp,
    ROUND(100.0 * null_battery / NULLIF(total_events, 0), 2) AS pct_missing_battery,
    ROUND(100.0 * null_is_cache / NULLIF(total_events, 0), 2) AS pct_missing_cache_flag,
    ROUND(100.0 * null_start_hole / NULLIF(total_events, 0), 2) AS pct_missing_start_hole,
    ROUND(100.0 * null_is_nine_hole / NULLIF(total_events, 0), 2) AS pct_missing_nine_hole_flag,
    ROUND(100.0 * null_is_complete / NULLIF(total_events, 0), 2) AS pct_missing_complete_flag,
    ROUND(100.0 * timestamp_missing_flag / NULLIF(total_events, 0), 2) AS pct_problem_events,
    ROUND(100 - (
        (COALESCE(100.0 * null_pace / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_pace_gap / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_positional_gap / NULLIF(total_events, 0), 0) +
         COALESCE(100.0 * null_goal_time / NULLIF(total_events, 0), 0)) / 4
    ), 1) AS data_quality_score
FROM course_stats
ORDER BY data_quality_score ASC
"""

DBT_CRITICAL_COLUMN_GAPS = """
-- dbt model: pipeline/gold/models/gold/critical_column_gaps.sql
-- This is the transformation from silver -> gold
WITH column_analysis AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT round_id) AS total_rounds,
        SUM(CASE WHEN pace IS NULL THEN 1 ELSE 0 END) AS t1_null_pace,
        SUM(CASE WHEN pace_gap IS NULL THEN 1 ELSE 0 END) AS t1_null_pace_gap,
        SUM(CASE WHEN positional_gap IS NULL THEN 1 ELSE 0 END) AS t1_null_positional_gap,
        SUM(CASE WHEN hole_number IS NULL THEN 1 ELSE 0 END) AS t2_null_hole,
        SUM(CASE WHEN section_number IS NULL THEN 1 ELSE 0 END) AS t2_null_section,
        SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) AS t2_null_lat,
        SUM(CASE WHEN fix_timestamp IS NULL THEN 1 ELSE 0 END) AS t2_null_timestamp,
        SUM(CASE WHEN battery_percentage IS NULL THEN 1 ELSE 0 END) AS t3_null_battery,
        SUM(CASE WHEN is_cache IS NULL THEN 1 ELSE 0 END) AS t3_null_cache,
        SUM(CASE WHEN start_hole IS NULL THEN 1 ELSE 0 END) AS t4_null_start_hole,
        SUM(CASE WHEN is_nine_hole IS NULL THEN 1 ELSE 0 END) AS t4_null_is_nine_hole,
        SUM(CASE WHEN is_complete IS NULL THEN 1 ELSE 0 END) AS t4_null_is_complete,
        SUM(CASE WHEN goal_time IS NULL THEN 1 ELSE 0 END) AS t4_null_goal_time
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
    GROUP BY course_id
)
SELECT
    course_id,
    total_events,
    total_rounds,
    ROUND(100.0 * t1_null_pace / total_events, 2) AS pct_null_pace,
    ROUND(100.0 * t1_null_pace_gap / total_events, 2) AS pct_null_pace_gap,
    ROUND(100.0 * t1_null_positional_gap / total_events, 2) AS pct_null_positional_gap,
    CASE 
        WHEN 100.0 * GREATEST(t1_null_pace, t1_null_pace_gap) / total_events > 50 THEN '🔴 CRITICAL: Pace analysis NOT possible'
        WHEN 100.0 * GREATEST(t1_null_pace, t1_null_pace_gap) / total_events > 20 THEN '🟠 WARNING: Pace analysis degraded'
        WHEN 100.0 * GREATEST(t1_null_pace, t1_null_pace_gap) / total_events > 5 THEN '🟡 MINOR: Some pace gaps'
        ELSE '🟢 GOOD: Pace data complete'
    END AS pace_data_status,
    ROUND(100.0 * t2_null_hole / total_events, 2) AS pct_null_hole,
    ROUND(100.0 * t2_null_section / total_events, 2) AS pct_null_section,
    ROUND(100.0 * t2_null_lat / total_events, 2) AS pct_null_latitude,
    ROUND(100.0 * t2_null_timestamp / total_events, 2) AS pct_null_timestamp,
    CASE 
        WHEN 100.0 * t2_null_hole / total_events > 30 THEN '🔴 CRITICAL: Hole tracking broken'
        WHEN 100.0 * t2_null_hole / total_events > 10 THEN '🟠 WARNING: Location gaps detected'
        ELSE '🟢 GOOD: Location data complete'
    END AS location_data_status,
    ROUND(100.0 * t3_null_battery / total_events, 2) AS pct_null_battery,
    ROUND(100.0 * t3_null_cache / total_events, 2) AS pct_null_cache_flag,
    CASE 
        WHEN 100.0 * t3_null_battery / total_events > 50 THEN '🟠 WARNING: Cannot monitor device health'
        WHEN 100.0 * t3_null_battery / total_events > 20 THEN '🟡 MINOR: Some battery data missing'
        ELSE '🟢 GOOD: Device health trackable'
    END AS device_health_status,
    ROUND(100.0 * t4_null_start_hole / total_events, 2) AS pct_null_start_hole,
    ROUND(100.0 * t4_null_is_nine_hole / total_events, 2) AS pct_null_nine_hole_flag,
    ROUND(100.0 * t4_null_goal_time / total_events, 2) AS pct_null_goal_time,
    CASE 
        WHEN 100.0 * (t4_null_goal_time + t4_null_start_hole) / (2 * total_events) > 80 THEN '🟠 WARNING: Goal times not set'
        WHEN 100.0 * (t4_null_goal_time + t4_null_start_hole) / (2 * total_events) > 50 THEN '🟡 MINOR: Start hole unknown'
        ELSE '🟢 GOOD: Round config available'
    END AS round_config_status,
    ROUND(100 - (
        0.40 * (100.0 * CASE 
            WHEN (t1_null_pace + t1_null_pace_gap) > 0 THEN GREATEST(t1_null_pace, t1_null_pace_gap) 
            ELSE 0 
        END / total_events) +
        0.30 * (100.0 * (t2_null_hole + t2_null_timestamp) / (2 * total_events)) +
        0.20 * (100.0 * t3_null_battery / total_events) +
        0.10 * (100.0 * (t4_null_goal_time + t4_null_start_hole) / (2 * total_events))
    ), 1) AS usability_score,
    CASE 
        WHEN 100.0 * GREATEST(t1_null_pace, t1_null_pace_gap) / total_events > 20 
        THEN 'Check pace calculation algorithm - many events missing pace values'
        WHEN 100.0 * t2_null_hole / total_events > 20 
        THEN 'Review location assignment logic - many events without hole numbers'
        WHEN 100.0 * t3_null_battery / total_events > 50 
        THEN 'Enable battery reporting on devices'
        WHEN 100.0 * (t4_null_goal_time + t4_null_start_hole) / (2 * total_events) > 80 
        THEN 'Configure goal times for this course in the system'
        ELSE 'Data quality acceptable - monitor for changes'
    END AS top_recommendation
FROM column_analysis
ORDER BY usability_score ASC
"""

DBT_COURSE_CONFIGURATION = """
-- dbt model: pipeline/gold/models/gold/course_configuration_analysis.sql
-- This is the transformation from silver -> gold
WITH round_configs AS (
    SELECT
        course_id,
        round_id,
        start_hole,
        is_nine_hole,
        is_complete,
        MIN(section_number) AS min_section,
        MAX(section_number) AS max_section,
        MIN(nine_number) AS min_nine,
        MAX(nine_number) AS max_nine,
        COUNT(DISTINCT hole_number) AS unique_holes_played,
        COUNT(DISTINCT nine_number) AS nines_played,
        COUNT(*) AS location_count
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND hole_number IS NOT NULL
    GROUP BY course_id, round_id, start_hole, is_nine_hole, is_complete
),
course_summary AS (
    SELECT
        course_id,
        COUNT(DISTINCT round_id) AS total_rounds,
        MAX(max_section) AS max_section_seen,
        MAX(unique_holes_played) AS max_holes_in_round,
        MAX(nines_played) AS max_nines_in_round,
        CASE 
            WHEN MAX(max_section) > 54 THEN '27-hole'
            WHEN MAX(max_section) > 27 THEN '18-hole'
            ELSE '9-hole'
        END AS likely_course_type,
        SUM(CASE WHEN is_nine_hole = TRUE THEN 1 ELSE 0 END) AS nine_hole_rounds,
        SUM(CASE WHEN is_nine_hole = FALSE OR is_nine_hole IS NULL THEN 1 ELSE 0 END) AS full_rounds,
        SUM(CASE WHEN is_complete = TRUE THEN 1 ELSE 0 END) AS complete_rounds,
        SUM(CASE WHEN is_complete = FALSE THEN 1 ELSE 0 END) AS incomplete_rounds,
        COUNT(DISTINCT start_hole) AS unique_start_holes,
        SUM(CASE WHEN start_hole = 1 THEN 1 ELSE 0 END) AS rounds_starting_hole_1,
        SUM(CASE WHEN start_hole != 1 AND start_hole IS NOT NULL THEN 1 ELSE 0 END) AS shotgun_start_rounds,
        SUM(CASE WHEN nines_played = 1 THEN 1 ELSE 0 END) AS single_nine_rounds,
        SUM(CASE WHEN nines_played = 2 THEN 1 ELSE 0 END) AS two_nine_rounds,
        SUM(CASE WHEN nines_played >= 3 THEN 1 ELSE 0 END) AS three_nine_rounds,
        ROUND(AVG(location_count), 0) AS avg_locations_per_round,
        MIN(location_count) AS min_locations_per_round,
        MAX(location_count) AS max_locations_per_round
    FROM round_configs
    GROUP BY course_id
)
SELECT
    course_id,
    total_rounds,
    likely_course_type,
    max_section_seen,
    max_holes_in_round,
    ROUND(100.0 * complete_rounds / NULLIF(total_rounds, 0), 1) AS pct_complete,
    ROUND(100.0 * incomplete_rounds / NULLIF(total_rounds, 0), 1) AS pct_incomplete,
    ROUND(100.0 * nine_hole_rounds / NULLIF(total_rounds, 0), 1) AS pct_nine_hole,
    ROUND(100.0 * full_rounds / NULLIF(total_rounds, 0), 1) AS pct_full_rounds,
    unique_start_holes,
    ROUND(100.0 * shotgun_start_rounds / NULLIF(total_rounds, 0), 1) AS pct_shotgun_starts,
    CASE WHEN likely_course_type = '27-hole' THEN
        ROUND(100.0 * single_nine_rounds / NULLIF(total_rounds, 0), 1)
    END AS pct_single_nine,
    CASE WHEN likely_course_type = '27-hole' THEN
        ROUND(100.0 * two_nine_rounds / NULLIF(total_rounds, 0), 1)
    END AS pct_two_nines,
    CASE WHEN likely_course_type = '27-hole' THEN
        ROUND(100.0 * three_nine_rounds / NULLIF(total_rounds, 0), 1)
    END AS pct_all_three_nines,
    avg_locations_per_round,
    min_locations_per_round,
    max_locations_per_round,
    ROUND(
        unique_start_holes * 10 +
        CASE likely_course_type 
            WHEN '27-hole' THEN 30 
            WHEN '18-hole' THEN 20 
            ELSE 10 
        END +
        CASE WHEN 100.0 * nine_hole_rounds / NULLIF(total_rounds, 0) > 20 THEN 10 ELSE 0 END +
        CASE WHEN 100.0 * incomplete_rounds / NULLIF(total_rounds, 0) > 10 THEN 5 ELSE 0 END
    , 0) AS course_complexity_score
FROM course_summary
ORDER BY course_complexity_score DESC
"""

DBT_PACE_SUMMARY = """
-- dbt model: pipeline/gold/models/gold/pace_summary_by_round.sql
-- This is the transformation from silver -> gold
WITH base AS (
    SELECT
        course_id,
        round_id,
        MIN(fix_timestamp) AS round_start_ts,
        MAX(fix_timestamp) AS round_end_ts,
        COUNT(*) AS fix_count,
        AVG(pace) AS avg_pace,
        AVG(pace_gap) AS avg_pace_gap,
        AVG(positional_gap) AS avg_positional_gap
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    round_id,
    round_start_ts,
    round_end_ts,
    fix_count,
    avg_pace,
    avg_pace_gap,
    avg_positional_gap
FROM base
"""

DBT_SIGNAL_QUALITY = """
-- dbt model: pipeline/gold/models/gold/signal_quality_rounds.sql
-- This is the transformation from silver -> gold
WITH base AS (
    SELECT
        course_id,
        round_id,
        COUNT(*) AS fix_count,
        SUM(CASE WHEN is_projected THEN 1 ELSE 0 END) AS projected_fix_count,
        SUM(CASE WHEN is_problem THEN 1 ELSE 0 END) AS problem_fix_count
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    round_id,
    fix_count,
    projected_fix_count,
    problem_fix_count,
    CAST(projected_fix_count AS DOUBLE) / NULLIF(fix_count, 0) AS projected_rate,
    CAST(problem_fix_count AS DOUBLE) / NULLIF(fix_count, 0) AS problem_rate
FROM base
"""
