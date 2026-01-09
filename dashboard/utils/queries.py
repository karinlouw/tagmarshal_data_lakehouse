"""SQL queries for the data quality dashboard.

All queries target the Silver layer directly to avoid dependencies on
Gold dbt models. This makes the dashboard more reliable and easier to debug.
"""

# =============================================================================
# OVERVIEW QUERIES
# =============================================================================

OVERVIEW_STATS = """
-- High-level statistics from Silver telemetry
    SELECT
        COUNT(DISTINCT course_id) AS total_courses,
    COUNT(DISTINCT round_id) AS total_rounds,
    COUNT(*) AS total_events,
    SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
    MIN(event_date) AS earliest_date,
    MAX(event_date) AS latest_date
FROM iceberg.silver.fact_telemetry_event
"""

COURSE_SUMMARY = """
-- Summary statistics per course
SELECT
    course_id,
    COUNT(DISTINCT round_id) AS round_count,
    COUNT(*) AS event_count,
    SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
    MIN(event_date) AS first_date,
    MAX(event_date) AS last_date,
    date_diff('day', MIN(event_date), MAX(event_date)) + 1 AS total_days,
    COUNT(DISTINCT CASE WHEN is_location_padding = FALSE THEN event_date END) AS playing_days,
    MAX(section_number) AS max_section,
    MAX(hole_number) AS max_hole,
    CASE 
        WHEN MAX(hole_number) >= 10 THEN '18-hole'
        WHEN MAX(section_number) > 54 THEN '27-hole'
        WHEN MAX(section_number) > 27 THEN '18-hole (loop)'
        ELSE '9-hole'
    END AS inferred_type
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY course_id
"""

# =============================================================================
# DATA QUALITY QUERIES
# =============================================================================

DATA_QUALITY_SCORE = """
-- Data quality score per course (0-100 composite score)
-- Weights: Core Telemetry (40%), Position Tracking (25%), Round Context (20%), Device Health (15%)
-- Core Telemetry: pace, pace_gap, positional_gap, GPS (lat/long), fix_timestamp
-- Position Tracking: hole_number, section_number, location_index, current_hole, current_hole_section
-- Round Context: round_start_time, round_end_time, start_hole, start_section, is_complete
-- Device Health: device, battery_percentage
WITH quality_metrics AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        -- Core Telemetry Metrics
        ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS pace_pct,
        ROUND(100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS pace_gap_pct,
        ROUND(100.0 * SUM(CASE WHEN positional_gap IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS positional_gap_pct,
        ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS gps_complete_pct,
        ROUND(100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS fix_timestamp_pct,
        -- Position Tracking Metrics
        ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS hole_pct,
        ROUND(100.0 * SUM(CASE WHEN section_number IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS section_pct,
        ROUND(100.0 * SUM(CASE WHEN location_index IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS location_index_pct,
        ROUND(100.0 * SUM(CASE WHEN current_hole IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS current_hole_pct,
        ROUND(100.0 * SUM(CASE WHEN current_hole_section IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS current_hole_section_pct,
        -- Round Context Metrics
        ROUND(100.0 * SUM(CASE WHEN round_start_time IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS round_start_time_pct,
        ROUND(100.0 * SUM(CASE WHEN round_end_time IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS round_end_time_pct,
        ROUND(100.0 * SUM(CASE WHEN start_hole IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS start_hole_pct,
        ROUND(100.0 * SUM(CASE WHEN start_section IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS start_section_pct,
        ROUND(100.0 * SUM(CASE WHEN is_complete IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS is_complete_pct,
        ROUND(100.0 * SUM(CASE WHEN goal_name IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS goal_name_pct,
        ROUND(100.0 * SUM(CASE WHEN is_projected IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS is_projected_pct,
        ROUND(100.0 * SUM(CASE WHEN is_problem IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS is_problem_pct,
        -- Device Health Metrics
        ROUND(100.0 * SUM(CASE WHEN device IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS device_pct,
        ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS battery_pct,
        -- Composite Quality Score
        ROUND(
            -- Core Telemetry (40%): pace metrics, GPS, timestamp
            0.15 * (100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.05 * (100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.05 * (100.0 * SUM(CASE WHEN positional_gap IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.10 * (100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.05 * (100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            -- Position Tracking (25%): hole, section, location tracking
            0.08 * (100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.08 * (100.0 * SUM(CASE WHEN section_number IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.05 * (100.0 * SUM(CASE WHEN location_index IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.02 * (100.0 * SUM(CASE WHEN current_hole IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.02 * (100.0 * SUM(CASE WHEN current_hole_section IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            -- Round Context (20%): round timing and metadata
            0.05 * (100.0 * SUM(CASE WHEN round_start_time IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.05 * (100.0 * SUM(CASE WHEN round_end_time IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.04 * (100.0 * SUM(CASE WHEN start_hole IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.03 * (100.0 * SUM(CASE WHEN start_section IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.03 * (100.0 * SUM(CASE WHEN is_complete IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            -- Device Health (15%): device identification and battery
            0.10 * (100.0 * SUM(CASE WHEN device IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) +
            0.05 * (100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0))
        , 1) AS quality_score
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
    GROUP BY course_id
)
SELECT
    course_id,
    total_events,
    -- Core Telemetry Metrics
    pace_pct,
    pace_gap_pct,
    positional_gap_pct,
    gps_complete_pct,
    fix_timestamp_pct,
    -- Position Tracking Metrics
    hole_pct,
    section_pct,
    location_index_pct,
    current_hole_pct,
    current_hole_section_pct,
    -- Round Context Metrics
    round_start_time_pct,
    round_end_time_pct,
    start_hole_pct,
    start_section_pct,
    is_complete_pct,
    goal_name_pct,
    is_projected_pct,
    is_problem_pct,
    -- Device Health Metrics
    device_pct,
    battery_pct,
    -- Composite Quality Score
    quality_score,
    -- Quality Category
    CASE
        WHEN quality_score >= 90 THEN 'Excellent'
        WHEN quality_score >= 75 THEN 'Good'
        WHEN quality_score >= 60 THEN 'Fair'
        WHEN quality_score >= 40 THEN 'Poor'
        ELSE 'Critical'
    END AS quality_category
FROM quality_metrics
ORDER BY quality_score DESC
"""

# A compact version used in the dashboard table (stable shape = easier UI code).
COLUMN_COMPLETENESS = """
-- Column completeness by course (percentage of non-null values)
SELECT
    course_id,
    COUNT(*) AS total_events,
    ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pace_pct,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pace_gap_pct,
    ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS hole_pct,
    ROUND(100.0 * SUM(CASE WHEN section_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS section_pct,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS gps_pct,
    ROUND(100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS fix_timestamp_pct,
    ROUND(100.0 * SUM(CASE WHEN start_hole IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS start_hole_pct,
    ROUND(100.0 * SUM(CASE WHEN start_section IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS start_section_pct,
    ROUND(100.0 * SUM(CASE WHEN is_complete IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS is_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS battery_pct,
    ROUND(100.0 * SUM(CASE WHEN device IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS device_pct
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
GROUP BY course_id
ORDER BY course_id
"""

# A detailed version you can use later if you want deeper debugging in the UI.
COLUMN_COMPLETENESS_EXTENDED = """
-- Column completeness by course (percentage of non-null values)
-- Ordered to match DATA_QUALITY_SCORE grouping: Core Telemetry, Position Tracking, Round Context, Device Health
  SELECT
    course_id,
    COUNT(*) AS total_events,
    -- Core Telemetry Metrics
    ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pace_pct,
    ROUND(100.0 * SUM(CASE WHEN pace_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pace_gap_pct,
    ROUND(100.0 * SUM(CASE WHEN positional_gap IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS positional_gap_pct,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS latitude_pct,
    ROUND(100.0 * SUM(CASE WHEN longitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS longitude_pct,
    ROUND(100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS fix_timestamp_pct,
    -- Position Tracking Metrics
    ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS hole_pct,
    ROUND(100.0 * SUM(CASE WHEN section_number IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS section_pct,
    ROUND(100.0 * SUM(CASE WHEN location_index IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS location_index_pct,
    ROUND(100.0 * SUM(CASE WHEN current_hole IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS current_hole_pct,
    ROUND(100.0 * SUM(CASE WHEN current_hole_section IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS current_hole_section_pct,
    -- Round Context Metrics
    ROUND(100.0 * SUM(CASE WHEN round_start_time IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS round_start_time_pct,
    ROUND(100.0 * SUM(CASE WHEN round_end_time IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS round_end_time_pct,
    ROUND(100.0 * SUM(CASE WHEN start_hole IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS start_hole_pct,
    ROUND(100.0 * SUM(CASE WHEN start_section IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS start_section_pct,
    ROUND(100.0 * SUM(CASE WHEN is_complete IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS is_complete_pct,
    ROUND(100.0 * SUM(CASE WHEN goal_name IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS goal_name_pct,
    ROUND(100.0 * SUM(CASE WHEN is_projected IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS is_projected_pct,
    ROUND(100.0 * SUM(CASE WHEN is_problem IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS is_problem_pct,
    ROUND(100.0 * SUM(CASE WHEN goal_time IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS goal_time_pct,
    ROUND(100.0 * SUM(CASE WHEN end_section IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS end_section_pct,
    -- Device Health Metrics
    ROUND(100.0 * SUM(CASE WHEN device IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS device_pct,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS battery_pct
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
GROUP BY course_id
ORDER BY course_id
"""

PADDING_ANALYSIS = """
-- Padding data analysis by course
SELECT
    course_id,
    COUNT(*) AS total_events,
    SUM(CASE WHEN is_location_padding = TRUE THEN 1 ELSE 0 END) AS padding_events,
    SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
    ROUND(100.0 * SUM(CASE WHEN is_location_padding = TRUE THEN 1 ELSE 0 END) / COUNT(*), 1) AS padding_pct
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY padding_pct DESC
"""

# =============================================================================
# TOPOLOGY QUERIES
# =============================================================================

TOPOLOGY = """
-- Course topology (nine boundaries)
SELECT
    facility_id AS course_id,
    unit_name,
    nine_number,
    section_start,
    section_end,
    section_end - section_start + 1 AS sections_in_nine
FROM iceberg.silver.dim_facility_topology
ORDER BY facility_id, nine_number
"""

# =============================================================================
# COURSE PROFILE QUERIES
# =============================================================================

COURSE_PROFILE = """
-- Course profile metadata (human-entered)
SELECT
    course_id,
    course_type,
    COALESCE(is_loop_course, FALSE) AS is_loop_course,
    volume_profile,
    peak_season_start_month,
    peak_season_end_month,
    notes
FROM iceberg.silver.dim_course_profile
ORDER BY course_id
"""

LOOP_COURSES = """
-- Get courses marked as loop courses (9-hole courses supporting 18-hole rounds)
SELECT
    course_id,
    course_type,
    notes
FROM iceberg.silver.dim_course_profile
WHERE is_loop_course = TRUE
   OR course_type LIKE '%loop%'
ORDER BY course_id
"""

COURSE_SUMMARY_WITH_PROFILE = """
-- Course summary with profile metadata
SELECT
    cs.course_id,
    cs.round_count,
    cs.event_count,
    cs.real_events,
    cs.first_date,
    cs.last_date,
    cs.total_days,
    cs.playing_days,
    cs.inferred_type,
    COALESCE(cp.course_type, cs.inferred_type) AS course_type,
    COALESCE(cp.is_loop_course, FALSE) AS is_loop_course,
    cp.volume_profile,
    cp.notes
FROM (
    SELECT
        course_id,
        COUNT(DISTINCT round_id) AS round_count,
        COUNT(*) AS event_count,
        SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
        MIN(event_date) AS first_date,
        MAX(event_date) AS last_date,
        date_diff('day', MIN(event_date), MAX(event_date)) + 1 AS total_days,
        COUNT(DISTINCT CASE WHEN is_location_padding = FALSE THEN event_date END) AS playing_days,
        CASE 
            WHEN MAX(hole_number) >= 10 THEN '18-hole'
            WHEN MAX(section_number) > 54 THEN '27-hole'
            WHEN MAX(section_number) > 27 THEN '18-hole (loop)'
            ELSE '9-hole'
        END AS inferred_type
    FROM iceberg.silver.fact_telemetry_event
    GROUP BY course_id
) cs
LEFT JOIN iceberg.silver.dim_course_profile cp ON cs.course_id = cp.course_id
ORDER BY cs.course_id
"""

SECTIONS_PER_HOLE = """
-- Sections per hole dimension table
-- Computed from fact_telemetry_event if dim_sections_per_hole doesn't exist
SELECT
    course_id,
    hole_number,
    MIN(section_number) AS section_start,
    MAX(section_number) AS section_end,
    COUNT(DISTINCT section_number) AS sections_count,
    CASE 
        WHEN MIN(section_number) IS NOT NULL AND MAX(section_number) IS NOT NULL 
        THEN MAX(section_number) - MIN(section_number) + 1 
        ELSE NULL 
    END AS section_range
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND hole_number IS NOT NULL
  AND section_number IS NOT NULL
GROUP BY course_id, hole_number
ORDER BY course_id, hole_number
"""

# Prefer the precomputed dimension if it exists (created by `just generate-sections-per-hole`).
SECTIONS_PER_HOLE_DIM = """
SELECT
    course_id,
    hole_number,
    section_start,
    section_end,
    sections_count,
    section_end - section_start + 1 AS section_range
FROM iceberg.silver.dim_sections_per_hole
ORDER BY course_id, hole_number
"""

# =============================================================================
# ROUND ANALYSIS QUERIES
# =============================================================================

ROUND_TYPES = """
-- Round type distribution by course
SELECT
    course_id,
    COUNT(DISTINCT round_id) AS total_rounds,
    SUM(CASE WHEN is_nine_hole = TRUE THEN 1 ELSE 0 END) AS nine_hole_rounds,
    SUM(CASE WHEN is_nine_hole = FALSE THEN 1 ELSE 0 END) AS full_rounds,
    SUM(CASE WHEN is_complete = TRUE THEN 1 ELSE 0 END) AS complete_rounds,
    SUM(CASE WHEN start_hole != 1 THEN 1 ELSE 0 END) AS shotgun_starts
FROM (
SELECT
    course_id,
    round_id,
        MAX(CAST(is_nine_hole AS INTEGER)) AS is_nine_hole,
        MAX(CAST(is_complete AS INTEGER)) AS is_complete,
        MIN(start_hole) AS start_hole
    FROM iceberg.silver.fact_telemetry_event
    GROUP BY course_id, round_id
)
GROUP BY course_id
ORDER BY course_id
"""

ROUND_DURATION = """
-- Round duration statistics by course
SELECT 
    course_id,
    COUNT(DISTINCT round_id) AS rounds_with_duration,
    ROUND(AVG(round_duration_minutes), 0) AS avg_duration_min,
    ROUND(MIN(round_duration_minutes), 0) AS min_duration_min,
    ROUND(MAX(round_duration_minutes), 0) AS max_duration_min
FROM iceberg.silver.fact_telemetry_event
WHERE round_duration_minutes IS NOT NULL
  AND round_duration_minutes > 0
  AND round_duration_minutes < 600  -- Filter unrealistic durations
GROUP BY course_id
ORDER BY course_id
"""

# =============================================================================
# SAMPLE DATA QUERIES
# =============================================================================


def get_round_sample(course_id: str, round_id: str = None) -> str:
    """Get a sample of telemetry data for a round, ordered logically."""
    if round_id:
        where = f"WHERE course_id = '{course_id}' AND round_id = '{round_id}'"
    else:
        where = f"WHERE course_id = '{course_id}'"

    return f"""
SELECT 
        round_id,
        location_index,
    hole_number,
    section_number,
        nine_number,
        pace,
        fix_timestamp,
        is_location_padding
FROM iceberg.silver.fact_telemetry_event 
    {where}
    ORDER BY round_id, hole_number NULLS LAST, section_number NULLS LAST, location_index
    LIMIT 100
    """


ROUND_LIST = """
-- List of rounds for exploration
SELECT DISTINCT
    course_id,
    round_id,
    MIN(event_date) AS round_date,
    COUNT(*) AS event_count
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
GROUP BY course_id, round_id
ORDER BY course_id, MIN(event_date) DESC
LIMIT 100
"""

# =============================================================================
# DEVICE HEALTH QUERIES
# =============================================================================

DEVICE_STATS = """
-- Device statistics by course
SELECT 
    course_id,
    COUNT(DISTINCT device) AS unique_devices,
    ROUND(AVG(battery_percentage), 1) AS avg_battery,
    ROUND(MIN(battery_percentage), 1) AS min_battery,
    COUNT(*) AS total_events,
    SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) AS low_battery_events,
    ROUND(100.0 * SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) / COUNT(*), 1) AS low_battery_pct,
    SUM(CASE WHEN is_cache = TRUE THEN 1 ELSE 0 END) AS cached_events,
    ROUND(100.0 * SUM(CASE WHEN is_cache = TRUE THEN 1 ELSE 0 END) / COUNT(*), 1) AS cached_pct,
    SUM(CASE WHEN is_problem = TRUE THEN 1 ELSE 0 END) AS problem_events,
    ROUND(100.0 * SUM(CASE WHEN is_problem = TRUE THEN 1 ELSE 0 END) / COUNT(*), 1) AS problem_pct,
    SUM(CASE WHEN is_projected = TRUE THEN 1 ELSE 0 END) AS projected_events,
    ROUND(100.0 * SUM(CASE WHEN is_projected = TRUE THEN 1 ELSE 0 END) / COUNT(*), 1) AS projected_pct
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
GROUP BY course_id
ORDER BY course_id
"""

# =============================================================================
# TOPOLOGY MAP QUERIES (GPS)
# =============================================================================

COURSE_CENTROIDS = """
-- One point per course for map overview
SELECT
    course_id,
    ROUND(AVG(latitude), 6) AS latitude,
    ROUND(AVG(longitude), 6) AS longitude,
    COUNT(*) AS event_count,
    SUM(CASE WHEN is_projected = TRUE THEN 1 ELSE 0 END) AS projected_events
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
GROUP BY course_id
ORDER BY course_id
"""


def get_course_topology_map_points(course_id: str) -> str:
    """Return a small set of representative points to visualise a single course map.

    We aggregate telemetry to (nine, hole, section) centroids so the map stays fast and
    communicates topology (course structure) rather than raw point density.
    """
    safe_course_id = course_id.replace("'", "''")
    return f"""
SELECT
    course_id,
    nine_number,
    hole_number,
    section_number,
    ROUND(AVG(latitude), 6) AS latitude,
    ROUND(AVG(longitude), 6) AS longitude,
    COUNT(*) AS event_count,
    SUM(CASE WHEN is_projected = TRUE THEN 1 ELSE 0 END) AS projected_events,
    ROUND(100.0 * SUM(CASE WHEN is_projected = TRUE THEN 1 ELSE 0 END) / COUNT(*), 1) AS projected_pct
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND course_id = '{safe_course_id}'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND nine_number IS NOT NULL
  AND hole_number IS NOT NULL
  AND section_number IS NOT NULL
GROUP BY course_id, nine_number, hole_number, section_number
ORDER BY nine_number, hole_number, section_number
"""


# =============================================================================
# TIME PATTERN QUERIES
# =============================================================================

ROUNDS_BY_MONTH = """
-- Round distribution by month
SELECT
    course_id,
    event_year,
    event_month,
    COUNT(DISTINCT round_id) AS round_count
    FROM iceberg.silver.fact_telemetry_event
WHERE event_year IS NOT NULL
GROUP BY course_id, event_year, event_month
ORDER BY course_id, event_year, event_month
"""

ROUNDS_BY_WEEKDAY = """
-- Round distribution by day of week
SELECT
    course_id,
    event_weekday,
    COUNT(DISTINCT round_id) AS round_count
    FROM iceberg.silver.fact_telemetry_event
WHERE event_weekday IS NOT NULL
GROUP BY course_id, event_weekday
ORDER BY course_id, event_weekday
"""

# =============================================================================
# ROUND LENGTH / HOLES VISITED
# =============================================================================

ROUND_LENGTH_DISTRIBUTION = """
-- Count rounds by holes visited (non-padding events)
WITH round_holes AS (
    SELECT
        course_id,
        round_id,
        COUNT(DISTINCT hole_number) AS holes_visited
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND hole_number IS NOT NULL
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    CASE
        WHEN holes_visited < 9 THEN '<9'
        WHEN holes_visited = 9 THEN '9'
        WHEN holes_visited = 18 THEN '18'
        WHEN holes_visited = 27 THEN '27'
        WHEN holes_visited > 27 THEN '>27'
        ELSE 'other (10–26)'
    END AS round_length_bucket,
    COUNT(*) AS round_count
FROM round_holes
GROUP BY course_id, 2
ORDER BY course_id,
    CASE
        WHEN round_length_bucket = '<9' THEN 1
        WHEN round_length_bucket = '9' THEN 2
        WHEN round_length_bucket = '18' THEN 3
        WHEN round_length_bucket = '27' THEN 4
        WHEN round_length_bucket = '>27' THEN 5
        ELSE 6
    END
"""

# =============================================================================
# NINES PLAYED (KEY FOR 27-HOLE COURSES)
# =============================================================================

ROUND_NINE_COMBINATIONS = """
-- Which nines were played per round? Essential for 27-hole courses.
-- Groups rounds by the set of nine_number values observed (e.g. "1", "1+2", "1+2+3").
WITH round_nines AS (
    SELECT
        course_id,
        round_id,
        ARRAY_AGG(DISTINCT nine_number ORDER BY nine_number) AS nines_array,
        COUNT(DISTINCT nine_number) AS nines_count,
        COUNT(DISTINCT hole_number) AS holes_played
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND nine_number IS NOT NULL
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    -- Create a readable string like "1", "1+2", "1+2+3"
    ARRAY_JOIN(nines_array, '+') AS nines_played,
    nines_count,
    CASE
        WHEN holes_played < 9 THEN '<9'
        WHEN holes_played = 9 THEN '9'
        WHEN holes_played = 18 THEN '18'
        WHEN holes_played = 27 THEN '27'
        WHEN holes_played > 27 THEN '>27'
        ELSE 'other (10–26)'
    END AS holes_played_bucket,
    COUNT(*) AS round_count
FROM round_nines
GROUP BY course_id, nines_array, nines_count, 4
ORDER BY course_id, nines_count, nines_played
"""


def get_round_nine_combinations_for_course(course_id: str) -> str:
    """Return ROUND_NINE_COMBINATIONS filtered to a single course."""
    safe_course_id = course_id.replace("'", "''")
    return f"""
WITH round_nines AS (
    SELECT
        course_id,
        round_id,
        ARRAY_AGG(DISTINCT nine_number ORDER BY nine_number) AS nines_array,
        COUNT(DISTINCT nine_number) AS nines_count,
        COUNT(DISTINCT hole_number) AS holes_played
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND nine_number IS NOT NULL
      AND course_id = '{safe_course_id}'
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    ARRAY_JOIN(nines_array, '+') AS nines_played,
    nines_count,
    CASE
        WHEN holes_played < 9 THEN '<9'
        WHEN holes_played = 9 THEN '9'
        WHEN holes_played = 18 THEN '18'
        WHEN holes_played = 27 THEN '27'
        WHEN holes_played > 27 THEN '>27'
        ELSE 'other (10–26)'
    END AS holes_played_bucket,
    COUNT(*) AS round_count
FROM round_nines
GROUP BY course_id, nines_array, nines_count, 4
ORDER BY nines_count, nines_played
"""


# =============================================================================
# ROUND VALIDATION QUERIES
# =============================================================================

ROUND_VALIDATION = """
-- Round validation: check if rounds make logical sense
-- Validates: duration, sequence, completeness
WITH round_stats AS (
    SELECT
        course_id,
        round_id,
        MIN(fix_timestamp) AS first_fix,
        MAX(fix_timestamp) AS last_fix,
        MIN(round_start_time) AS round_start,
        MAX(round_end_time) AS round_end,
        MAX(round_duration_minutes) AS duration_minutes,
        MIN(start_hole) AS start_hole,
        MIN(hole_number) AS min_hole,
        MAX(hole_number) AS max_hole,
        MIN(section_number) AS min_section,
        MAX(section_number) AS max_section,
        COUNT(DISTINCT hole_number) AS holes_visited,
        COUNT(DISTINCT section_number) AS sections_visited,
        MAX(CAST(is_complete AS INTEGER)) AS is_complete,
        MAX(CAST(is_nine_hole AS INTEGER)) AS is_nine_hole,
        COUNT(*) AS event_count,
        SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
        SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) AS events_with_pace
    FROM iceberg.silver.fact_telemetry_event
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    round_id,
    duration_minutes,
    start_hole,
    min_hole,
    max_hole,
    holes_visited,
    sections_visited,
    event_count,
    real_events,
    is_complete,
    is_nine_hole,
    
    -- Duration validation (9-hole: 60-180 min, 18-hole: 120-360 min)
    CASE
        WHEN duration_minutes IS NULL THEN FALSE
        WHEN is_nine_hole = 1 AND duration_minutes BETWEEN 45 AND 200 THEN TRUE
        WHEN is_nine_hole = 0 AND duration_minutes BETWEEN 90 AND 400 THEN TRUE
        ELSE FALSE
    END AS duration_valid,
    
    -- Sequence validation: holes should be sequential from start
    CASE
        WHEN start_hole IS NULL THEN FALSE
        WHEN min_hole IS NULL THEN FALSE
        -- For shotgun starts, min_hole should match start_hole
        WHEN start_hole > 1 AND min_hole = start_hole THEN TRUE
        -- For normal starts, should start at hole 1
        WHEN start_hole = 1 AND min_hole = 1 THEN TRUE
        ELSE FALSE
    END AS sequence_valid,
    
    -- Completeness validation: should have reasonable event count
    CASE
        WHEN real_events < 10 THEN FALSE
        WHEN is_nine_hole = 1 AND real_events >= 9 THEN TRUE
        WHEN is_nine_hole = 0 AND real_events >= 18 THEN TRUE
        ELSE FALSE
    END AS events_valid,
    
    -- Pace data validation: should have some pace data
    CASE
        WHEN events_with_pace = 0 THEN FALSE
        WHEN CAST(events_with_pace AS DOUBLE) / NULLIF(real_events, 0) >= 0.5 THEN TRUE
        ELSE FALSE
    END AS pace_valid
    
FROM round_stats
ORDER BY course_id, round_id
"""


def get_round_validation_for_course(course_id: str) -> str:
    """Return the ROUND_VALIDATION query filtered to a single course.

    Why: The unfiltered ROUND_VALIDATION can be large. Filtering server-side keeps
    the dashboard responsive as data scales beyond the pilot.
    """
    # Basic single-quote escaping for safety in this demo dashboard.
    safe_course_id = course_id.replace("'", "''")
    return f"""
WITH round_stats AS (
    SELECT
        course_id,
        round_id,
        MIN(fix_timestamp) AS first_fix,
        MAX(fix_timestamp) AS last_fix,
        MIN(round_start_time) AS round_start,
        MAX(round_end_time) AS round_end,
        MAX(round_duration_minutes) AS duration_minutes,
        MIN(start_hole) AS start_hole,
        MIN(hole_number) AS min_hole,
        MAX(hole_number) AS max_hole,
        MIN(section_number) AS min_section,
        MAX(section_number) AS max_section,
        COUNT(DISTINCT hole_number) AS holes_visited,
        COUNT(DISTINCT section_number) AS sections_visited,
        MAX(CAST(is_complete AS INTEGER)) AS is_complete,
        MAX(CAST(is_nine_hole AS INTEGER)) AS is_nine_hole,
        COUNT(*) AS event_count,
        SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
        SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) AS events_with_pace
    FROM iceberg.silver.fact_telemetry_event
    WHERE course_id = '{safe_course_id}'
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    round_id,
    duration_minutes,
    start_hole,
    min_hole,
    max_hole,
    holes_visited,
    sections_visited,
    event_count,
    real_events,
    is_complete,
    is_nine_hole,
    CASE
        WHEN duration_minutes IS NULL THEN FALSE
        WHEN is_nine_hole = 1 AND duration_minutes BETWEEN 45 AND 200 THEN TRUE
        WHEN is_nine_hole = 0 AND duration_minutes BETWEEN 90 AND 400 THEN TRUE
        ELSE FALSE
    END AS duration_valid,
    CASE
        WHEN start_hole IS NULL THEN FALSE
        WHEN min_hole IS NULL THEN FALSE
        WHEN start_hole > 1 AND min_hole = start_hole THEN TRUE
        WHEN start_hole = 1 AND min_hole = 1 THEN TRUE
        ELSE FALSE
    END AS sequence_valid,
    CASE
        WHEN real_events < 10 THEN FALSE
        WHEN is_nine_hole = 1 AND real_events >= 9 THEN TRUE
        WHEN is_nine_hole = 0 AND real_events >= 18 THEN TRUE
        ELSE FALSE
    END AS events_valid,
    CASE
        WHEN events_with_pace = 0 THEN FALSE
        WHEN CAST(events_with_pace AS DOUBLE) / NULLIF(real_events, 0) >= 0.5 THEN TRUE
        ELSE FALSE
    END AS pace_valid
FROM round_stats
ORDER BY round_id
"""


ROUND_VALIDATION_SUMMARY = """
-- Summary of round validation by course
WITH validations AS (
    SELECT
        course_id,
        round_id,
        round_duration_minutes,
        is_nine_hole,
        is_complete,
        real_events,
        events_with_pace,
        -- Duration valid
        CASE
            WHEN round_duration_minutes IS NULL THEN 0
            WHEN is_nine_hole AND round_duration_minutes BETWEEN 45 AND 200 THEN 1
            WHEN NOT is_nine_hole AND round_duration_minutes BETWEEN 90 AND 400 THEN 1
            ELSE 0
        END AS duration_valid,
        -- Events valid
        CASE
            WHEN real_events < 10 THEN 0
            WHEN is_nine_hole AND real_events >= 9 THEN 1
            WHEN NOT is_nine_hole AND real_events >= 18 THEN 1
            ELSE 0
        END AS events_valid,
        -- Pace valid
        CASE
            WHEN events_with_pace = 0 THEN 0
            WHEN CAST(events_with_pace AS DOUBLE) / NULLIF(real_events, 0) >= 0.5 THEN 1
            ELSE 0
        END AS pace_valid
    FROM (
        SELECT
            course_id,
            round_id,
            MAX(round_duration_minutes) AS round_duration_minutes,
            MAX(CAST(is_nine_hole AS INTEGER)) = 1 AS is_nine_hole,
            MAX(CAST(is_complete AS INTEGER)) = 1 AS is_complete,
            SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
            SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) AS events_with_pace
        FROM iceberg.silver.fact_telemetry_event
        GROUP BY course_id, round_id
    )
)
SELECT
    course_id,
    COUNT(*) AS total_rounds,
    SUM(duration_valid) AS rounds_duration_valid,
    SUM(events_valid) AS rounds_events_valid,
    SUM(pace_valid) AS rounds_pace_valid,
    ROUND(100.0 * SUM(duration_valid) / COUNT(*), 1) AS pct_duration_valid,
    ROUND(100.0 * SUM(events_valid) / COUNT(*), 1) AS pct_events_valid,
    ROUND(100.0 * SUM(pace_valid) / COUNT(*), 1) AS pct_pace_valid
FROM validations
GROUP BY course_id
ORDER BY course_id
"""

# =============================================================================
# ROUND DURATION ANALYSIS (FOR OUTLIER DETECTION)
# =============================================================================

ROUND_DURATION_DETAILS = """
-- Detailed round duration for distribution and outlier analysis
WITH round_stats AS (
    SELECT
        course_id,
        round_id,
        MAX(round_duration_minutes) AS duration_minutes,
        MAX(CAST(is_nine_hole AS INTEGER)) = 1 AS is_nine_hole,
        COUNT(DISTINCT hole_number) AS holes_visited,
        MIN(event_date) AS round_date
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    round_id,
    round_date,
    duration_minutes,
    is_nine_hole,
    holes_visited,
    CASE WHEN is_nine_hole THEN '9-hole' ELSE '18-hole' END AS round_type
FROM round_stats
WHERE duration_minutes IS NOT NULL
  AND duration_minutes > 0
  AND duration_minutes < 600
ORDER BY course_id, round_date DESC
"""


def get_round_duration_for_course(course_id: str) -> str:
    """Get round duration details for a specific course."""
    safe_course_id = course_id.replace("'", "''")
    return f"""
WITH round_stats AS (
    SELECT
        course_id,
        round_id,
        MAX(round_duration_minutes) AS duration_minutes,
        MAX(CAST(is_nine_hole AS INTEGER)) = 1 AS is_nine_hole,
        COUNT(DISTINCT hole_number) AS holes_visited,
        MIN(event_date) AS round_date
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND course_id = '{safe_course_id}'
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    round_id,
    round_date,
    duration_minutes,
    is_nine_hole,
    holes_visited,
    CASE WHEN is_nine_hole THEN '9-hole' ELSE '18-hole' END AS round_type
FROM round_stats
WHERE duration_minutes IS NOT NULL
  AND duration_minutes > 0
  AND duration_minutes < 600
ORDER BY round_date DESC
"""


# =============================================================================
# HOLE DURATION ANALYSIS
# =============================================================================


def get_hole_duration_for_course(course_id: str) -> str:
    """Get per-hole duration statistics for a course."""
    safe_course_id = course_id.replace("'", "''")
    return f"""
WITH hole_times AS (
    SELECT
        course_id,
        round_id,
        hole_number,
        MIN(fix_timestamp) AS hole_start,
        MAX(fix_timestamp) AS hole_end
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND course_id = '{safe_course_id}'
      AND hole_number IS NOT NULL
      AND fix_timestamp IS NOT NULL
    GROUP BY course_id, round_id, hole_number
),
hole_durations AS (
    SELECT
        course_id,
        round_id,
        hole_number,
        date_diff('minute', hole_start, hole_end) AS hole_duration_minutes
    FROM hole_times
    WHERE hole_start IS NOT NULL AND hole_end IS NOT NULL
)
SELECT
    hole_number,
    COUNT(*) AS sample_count,
    ROUND(AVG(hole_duration_minutes), 1) AS avg_duration_min,
    ROUND(APPROX_PERCENTILE(hole_duration_minutes, 0.5), 1) AS median_duration_min,
    MIN(hole_duration_minutes) AS min_duration_min,
    MAX(hole_duration_minutes) AS max_duration_min,
    ROUND(STDDEV(hole_duration_minutes), 1) AS stddev_duration
FROM hole_durations
WHERE hole_duration_minutes > 0 AND hole_duration_minutes < 60
GROUP BY hole_number
ORDER BY hole_number
"""


# =============================================================================
# ROUND PROGRESSION ANALYSIS
# =============================================================================


def get_round_progression(course_id: str, round_id: str) -> str:
    """Get detailed progression data for a specific round to analyze sequence."""
    safe_course_id = course_id.replace("'", "''")
    safe_round_id = round_id.replace("'", "''")
    return f"""
SELECT
    location_index,
    nine_number,
    hole_number,
    section_number,
    fix_timestamp,
    pace,
    is_location_padding,
    ROW_NUMBER() OVER (ORDER BY location_index) AS event_sequence,
    LAG(hole_number) OVER (ORDER BY location_index) AS prev_hole,
    LAG(section_number) OVER (ORDER BY location_index) AS prev_section,
    CASE
        WHEN LAG(hole_number) OVER (ORDER BY location_index) IS NULL THEN 'start'
        WHEN hole_number = LAG(hole_number) OVER (ORDER BY location_index) THEN 'same_hole'
        WHEN hole_number = LAG(hole_number) OVER (ORDER BY location_index) + 1 THEN 'next_hole'
        WHEN hole_number < LAG(hole_number) OVER (ORDER BY location_index) THEN 'backwards'
        ELSE 'skip'
    END AS hole_transition,
    CASE
        WHEN LAG(section_number) OVER (ORDER BY location_index) IS NULL THEN 'start'
        WHEN section_number = LAG(section_number) OVER (ORDER BY location_index) THEN 'same_section'
        WHEN section_number = LAG(section_number) OVER (ORDER BY location_index) + 1 THEN 'next_section'
        WHEN section_number < LAG(section_number) OVER (ORDER BY location_index) THEN 'backwards'
        ELSE 'skip'
    END AS section_transition
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = '{safe_course_id}'
  AND round_id = '{safe_round_id}'
  AND is_location_padding = FALSE
ORDER BY location_index
"""


def get_round_map_points(course_id: str, round_id: str) -> str:
    """Get GPS points for a specific round to visualize on a map."""
    safe_course_id = course_id.replace("'", "''")
    safe_round_id = round_id.replace("'", "''")
    return f"""
SELECT
    location_index,
    latitude,
    longitude,
    nine_number,
    hole_number,
    section_number,
    fix_timestamp,
    pace,
    ROW_NUMBER() OVER (ORDER BY location_index) AS event_sequence
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = '{safe_course_id}'
  AND round_id = '{safe_round_id}'
  AND is_location_padding = FALSE
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
ORDER BY location_index
"""


def get_round_progression_summary(course_id: str) -> str:
    """Get progression quality summary for all rounds in a course."""
    safe_course_id = course_id.replace("'", "''")
    return f"""
WITH round_transitions AS (
    SELECT
        course_id,
        round_id,
        location_index,
        hole_number,
        section_number,
        LAG(hole_number) OVER (PARTITION BY round_id ORDER BY location_index) AS prev_hole,
        LAG(section_number) OVER (PARTITION BY round_id ORDER BY location_index) AS prev_section
    FROM iceberg.silver.fact_telemetry_event
    WHERE course_id = '{safe_course_id}'
      AND is_location_padding = FALSE
      AND hole_number IS NOT NULL
      AND section_number IS NOT NULL
),
transition_analysis AS (
    SELECT
        round_id,
        COUNT(*) AS total_events,
        SUM(CASE WHEN prev_hole IS NULL THEN 0
                 WHEN hole_number = prev_hole THEN 0
                 WHEN hole_number = prev_hole + 1 THEN 0
                 WHEN hole_number < prev_hole THEN 1
                 ELSE 1 END) AS hole_anomalies,
        SUM(CASE WHEN prev_section IS NULL THEN 0
                 WHEN section_number = prev_section THEN 0
                 WHEN section_number = prev_section + 1 THEN 0
                 WHEN section_number < prev_section THEN 1
                 ELSE 1 END) AS section_anomalies,
        MIN(hole_number) AS start_hole,
        MAX(hole_number) AS end_hole,
        COUNT(DISTINCT hole_number) AS holes_visited
    FROM round_transitions
    GROUP BY round_id
)
SELECT
    round_id,
    total_events,
    start_hole,
    end_hole,
    holes_visited,
    hole_anomalies,
    section_anomalies,
    ROUND(100.0 * hole_anomalies / NULLIF(total_events, 0), 1) AS hole_anomaly_pct,
    ROUND(100.0 * section_anomalies / NULLIF(total_events, 0), 1) AS section_anomaly_pct,
    CASE
        WHEN hole_anomalies = 0 AND section_anomalies = 0 THEN 'clean'
        WHEN hole_anomalies <= 2 AND section_anomalies <= 5 THEN 'minor_issues'
        ELSE 'needs_review'
    END AS progression_quality
FROM transition_analysis
ORDER BY hole_anomalies DESC, section_anomalies DESC
"""


# =============================================================================
# GLOBAL METRICS QUERIES (CROSS-COURSE INSIGHTS)
# =============================================================================
# These queries demonstrate the power of having all data in a single lakehouse
# vs siloed MongoDB databases per course.

GLOBAL_OVERVIEW = """
-- Global overview: aggregate stats across ALL courses
-- This query is IMPOSSIBLE with siloed databases!
SELECT
    COUNT(DISTINCT course_id) AS total_courses,
    COUNT(DISTINCT round_id) AS total_rounds,
    COUNT(*) AS total_events,
    SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
    COUNT(DISTINCT device) AS unique_devices,
    MIN(event_date) AS earliest_date,
    MAX(event_date) AS latest_date,
    COUNT(DISTINCT event_date) AS total_playing_days,
    ROUND(AVG(pace), 1) AS global_avg_pace,
    ROUND(AVG(battery_percentage), 1) AS global_avg_battery
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
"""

GLOBAL_PACE_COMPARISON = """
-- Compare pace metrics across ALL courses
-- Enables benchmarking that was impossible before
SELECT
    course_id,
    COUNT(DISTINCT round_id) AS round_count,
    ROUND(AVG(pace), 1) AS avg_pace,
    ROUND(APPROX_PERCENTILE(pace, 0.5), 1) AS median_pace,
    ROUND(MIN(pace), 1) AS min_pace,
    ROUND(MAX(pace), 1) AS max_pace,
    ROUND(STDDEV(pace), 1) AS pace_stddev,
    ROUND(AVG(pace_gap), 1) AS avg_pace_gap
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND pace IS NOT NULL
  AND pace > 0
  AND pace < 600  -- Filter unrealistic values
GROUP BY course_id
ORDER BY avg_pace
"""

GLOBAL_ROUND_DURATION_COMPARISON = """
-- Compare round durations across ALL courses
-- See which courses play faster/slower
WITH round_durations AS (
    SELECT
        course_id,
        round_id,
        MAX(round_duration_minutes) AS duration_minutes,
        MAX(CAST(is_nine_hole AS INTEGER)) = 1 AS is_nine_hole
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND round_duration_minutes IS NOT NULL
      AND round_duration_minutes > 30
      AND round_duration_minutes < 480
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    COUNT(*) AS round_count,
    ROUND(AVG(duration_minutes), 0) AS avg_duration_min,
    ROUND(APPROX_PERCENTILE(duration_minutes, 0.5), 0) AS median_duration_min,
    ROUND(MIN(duration_minutes), 0) AS min_duration_min,
    ROUND(MAX(duration_minutes), 0) AS max_duration_min,
    ROUND(STDDEV(duration_minutes), 0) AS duration_stddev,
    SUM(CASE WHEN is_nine_hole THEN 1 ELSE 0 END) AS nine_hole_rounds,
    SUM(CASE WHEN NOT is_nine_hole THEN 1 ELSE 0 END) AS full_rounds
FROM round_durations
GROUP BY course_id
ORDER BY avg_duration_min
"""

GLOBAL_WEEKDAY_HEATMAP = """
-- Rounds by weekday across ALL courses
-- Discover global patterns in play behavior
SELECT
    course_id,
    event_weekday,
    COUNT(DISTINCT round_id) AS round_count
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND event_weekday IS NOT NULL
GROUP BY course_id, event_weekday
ORDER BY course_id, event_weekday
"""

GLOBAL_HOURLY_DISTRIBUTION = """
-- Rounds by hour of day across ALL courses
-- When do golfers tee off globally?
SELECT
    course_id,
    HOUR(round_start_time) AS start_hour,
    COUNT(DISTINCT round_id) AS round_count
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND round_start_time IS NOT NULL
GROUP BY course_id, HOUR(round_start_time)
ORDER BY course_id, start_hour
"""

GLOBAL_DATA_QUALITY_RANKING = """
-- Rank courses by data quality score
-- Identify which courses need attention
WITH quality_metrics AS (
    SELECT
        course_id,
        COUNT(*) AS total_events,
        ROUND(100.0 * SUM(CASE WHEN pace IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS pace_completeness,
        ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS gps_completeness,
        ROUND(100.0 * SUM(CASE WHEN hole_number IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS hole_completeness,
        ROUND(100.0 * SUM(CASE WHEN fix_timestamp IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS timestamp_completeness
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
    GROUP BY course_id
)
SELECT
    course_id,
    total_events,
    pace_completeness,
    gps_completeness,
    hole_completeness,
    timestamp_completeness,
    ROUND((pace_completeness + gps_completeness + hole_completeness + timestamp_completeness) / 4, 1) AS avg_quality_score,
    RANK() OVER (ORDER BY (pace_completeness + gps_completeness + hole_completeness + timestamp_completeness) / 4 DESC) AS quality_rank
FROM quality_metrics
ORDER BY quality_rank
"""

GLOBAL_DEVICE_FLEET = """
-- Device fleet analysis across ALL courses
-- Understand device distribution and health globally
SELECT
    course_id,
    COUNT(DISTINCT device) AS unique_devices,
    COUNT(DISTINCT round_id) AS rounds_tracked,
    ROUND(CAST(COUNT(DISTINCT round_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT device), 0), 1) AS rounds_per_device,
    ROUND(AVG(battery_percentage), 1) AS avg_battery,
    ROUND(MIN(battery_percentage), 1) AS min_battery,
    SUM(CASE WHEN battery_percentage < 20 THEN 1 ELSE 0 END) AS low_battery_events,
    SUM(CASE WHEN is_problem = TRUE THEN 1 ELSE 0 END) AS problem_events
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
GROUP BY course_id
ORDER BY unique_devices DESC
"""

GLOBAL_MONTHLY_TREND = """
-- Monthly round trends across ALL courses
-- See seasonality patterns globally
SELECT
    event_year,
    event_month,
    course_id,
    COUNT(DISTINCT round_id) AS round_count
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND event_year IS NOT NULL
  AND event_month IS NOT NULL
GROUP BY event_year, event_month, course_id
ORDER BY event_year, event_month, course_id
"""

GLOBAL_COMPLETION_RATES = """
-- Round completion rates across ALL courses
-- Which courses have the most complete rounds?
WITH round_stats AS (
    SELECT
        course_id,
        round_id,
        MAX(CAST(is_complete AS INTEGER)) AS is_complete,
        COUNT(DISTINCT hole_number) AS holes_visited,
        MAX(CAST(is_nine_hole AS INTEGER)) AS is_nine_hole
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
    GROUP BY course_id, round_id
)
SELECT
    course_id,
    COUNT(*) AS total_rounds,
    SUM(is_complete) AS complete_rounds,
    ROUND(100.0 * SUM(is_complete) / NULLIF(COUNT(*), 0), 1) AS completion_rate,
    ROUND(AVG(holes_visited), 1) AS avg_holes_visited,
    SUM(is_nine_hole) AS nine_hole_rounds,
    SUM(CASE WHEN is_nine_hole = 0 THEN 1 ELSE 0 END) AS eighteen_hole_rounds
FROM round_stats
GROUP BY course_id
ORDER BY completion_rate DESC
"""

# =============================================================================
# PACE ANALYSIS QUERIES
# =============================================================================

PACE_BY_HOLE = """
-- Average pace by hole for each course
-- Identifies bottleneck holes (slower than course average)
WITH hole_pace AS (
    SELECT
        course_id,
        hole_number,
        pace,
        round_id
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND pace IS NOT NULL
      AND pace > 0
      AND pace < 60  -- Filter unrealistic values (> 1 hour per hole)
      AND hole_number IS NOT NULL
),
course_avg AS (
    SELECT
        course_id,
        ROUND(AVG(pace), 1) AS course_avg_pace
    FROM hole_pace
    GROUP BY course_id
)
SELECT
    hp.course_id,
    hp.hole_number,
    COUNT(DISTINCT hp.round_id) AS sample_rounds,
    COUNT(*) AS sample_events,
    ROUND(AVG(hp.pace), 1) AS avg_pace,
    ROUND(APPROX_PERCENTILE(hp.pace, 0.5), 1) AS median_pace,
    ROUND(MIN(hp.pace), 1) AS min_pace,
    ROUND(MAX(hp.pace), 1) AS max_pace,
    ROUND(STDDEV(hp.pace), 1) AS pace_stddev,
    ca.course_avg_pace,
    ROUND(AVG(hp.pace) - ca.course_avg_pace, 1) AS pace_vs_avg,
    CASE
        WHEN AVG(hp.pace) > ca.course_avg_pace * 1.15 THEN 'bottleneck'
        WHEN AVG(hp.pace) < ca.course_avg_pace * 0.85 THEN 'fast'
        ELSE 'normal'
    END AS hole_category
FROM hole_pace hp
JOIN course_avg ca ON hp.course_id = ca.course_id
GROUP BY hp.course_id, hp.hole_number, ca.course_avg_pace
ORDER BY hp.course_id, hp.hole_number
"""


def get_pace_by_hole_for_course(course_id: str) -> str:
    """Get pace by hole for a specific course."""
    safe_course_id = course_id.replace("'", "''")
    return f"""
WITH hole_pace AS (
    SELECT
        course_id,
        hole_number,
        pace,
        round_id
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND pace IS NOT NULL
      AND pace > 0
      AND pace < 60
      AND hole_number IS NOT NULL
      AND course_id = '{safe_course_id}'
),
course_avg AS (
    SELECT
        course_id,
        ROUND(AVG(pace), 1) AS course_avg_pace
    FROM hole_pace
    GROUP BY course_id
)
SELECT
    hp.course_id,
    hp.hole_number,
    COUNT(DISTINCT hp.round_id) AS sample_rounds,
    COUNT(*) AS sample_events,
    ROUND(AVG(hp.pace), 1) AS avg_pace,
    ROUND(APPROX_PERCENTILE(hp.pace, 0.5), 1) AS median_pace,
    ROUND(MIN(hp.pace), 1) AS min_pace,
    ROUND(MAX(hp.pace), 1) AS max_pace,
    ROUND(STDDEV(hp.pace), 1) AS pace_stddev,
    ca.course_avg_pace,
    ROUND(AVG(hp.pace) - ca.course_avg_pace, 1) AS pace_vs_avg,
    CASE
        WHEN AVG(hp.pace) > ca.course_avg_pace * 1.15 THEN 'bottleneck'
        WHEN AVG(hp.pace) < ca.course_avg_pace * 0.85 THEN 'fast'
        ELSE 'normal'
    END AS hole_category
FROM hole_pace hp
JOIN course_avg ca ON hp.course_id = ca.course_id
GROUP BY hp.course_id, hp.hole_number, ca.course_avg_pace
ORDER BY hp.hole_number
"""


PACE_BY_SECTION = """
-- Average pace by section for each course
-- More granular bottleneck detection
SELECT
    course_id,
    hole_number,
    section_number,
    COUNT(DISTINCT round_id) AS sample_rounds,
    ROUND(AVG(pace), 1) AS avg_pace,
    ROUND(APPROX_PERCENTILE(pace, 0.5), 1) AS median_pace
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND pace IS NOT NULL
  AND pace > 0
  AND pace < 60
  AND hole_number IS NOT NULL
  AND section_number IS NOT NULL
GROUP BY course_id, hole_number, section_number
ORDER BY course_id, hole_number, section_number
"""


def get_pace_by_section_for_course(course_id: str) -> str:
    """Get pace by section for a specific course."""
    safe_course_id = course_id.replace("'", "''")
    return f"""
SELECT
    course_id,
    hole_number,
    section_number,
    COUNT(DISTINCT round_id) AS sample_rounds,
    ROUND(AVG(pace), 1) AS avg_pace,
    ROUND(APPROX_PERCENTILE(pace, 0.5), 1) AS median_pace
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND pace IS NOT NULL
  AND pace > 0
  AND pace < 60
  AND hole_number IS NOT NULL
  AND section_number IS NOT NULL
  AND course_id = '{safe_course_id}'
GROUP BY course_id, hole_number, section_number
ORDER BY hole_number, section_number
"""


# Special query for 9-hole loop courses (like American Falls)
# Compares pace on the same hole between first nine and second nine
def get_nine_loop_pace_comparison(course_id: str) -> str:
    """Compare pace on the same hole between first and second nine for loop courses.

    For 9-hole courses that allow 18-hole rounds (playing the same 9 holes twice),
    this compares pace on each hole during the first pass vs second pass.

    Uses nine_number field: nine_number=1 is first pass, nine_number=2 is second pass.
    Filters for 18-hole rounds only (is_nine_hole = FALSE).
    """
    safe_course_id = course_id.replace("'", "''")
    return f"""
-- Get pace data from 18-hole rounds only, using nine_number to distinguish passes
-- nine_number=1 = first nine (first pass through holes 1-9)
-- nine_number=2 = second nine (second pass through holes 1-9)
SELECT
    hole_number,
    nine_number,
    CASE 
        WHEN nine_number = 1 THEN 'first_nine'
        WHEN nine_number = 2 THEN 'second_nine'
        ELSE 'nine_' || CAST(nine_number AS VARCHAR)
    END AS pass_number,
    COUNT(DISTINCT round_id) AS sample_rounds,
    COUNT(*) AS sample_events,
    ROUND(AVG(pace), 1) AS avg_pace,
    ROUND(APPROX_PERCENTILE(pace, 0.5), 1) AS median_pace,
    ROUND(MIN(pace), 1) AS min_pace,
    ROUND(MAX(pace), 1) AS max_pace,
    ROUND(STDDEV(pace), 1) AS pace_stddev
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND course_id = '{safe_course_id}'
  AND is_nine_hole = FALSE  -- Only 18-hole rounds
  AND pace IS NOT NULL
  AND pace > 0
  AND pace < 60
  AND hole_number IS NOT NULL
  AND nine_number IS NOT NULL
GROUP BY hole_number, nine_number
ORDER BY hole_number, nine_number
"""


def get_pace_comparison_for_hole(course_id: str, hole_number: int) -> str:
    """Get detailed pace comparison for a specific hole between first and second nine."""
    safe_course_id = course_id.replace("'", "''")
    return f"""
-- Get pace data for a specific hole from 18-hole rounds only
SELECT
    round_id,
    nine_number,
    CASE 
        WHEN nine_number = 1 THEN 'first_nine'
        WHEN nine_number = 2 THEN 'second_nine'
        ELSE 'nine_' || CAST(nine_number AS VARCHAR)
    END AS pass_number,
    pace,
    fix_timestamp
FROM iceberg.silver.fact_telemetry_event
WHERE is_location_padding = FALSE
  AND course_id = '{safe_course_id}'
  AND is_nine_hole = FALSE  -- Only 18-hole rounds
  AND pace IS NOT NULL
  AND pace > 0
  AND pace < 60
  AND hole_number = {hole_number}
  AND nine_number IS NOT NULL
ORDER BY round_id, fix_timestamp
"""


BOTTLENECK_SUMMARY = """
-- Summary of bottleneck holes across all courses
WITH hole_pace AS (
    SELECT
        course_id,
        hole_number,
        pace
    FROM iceberg.silver.fact_telemetry_event
    WHERE is_location_padding = FALSE
      AND pace IS NOT NULL
      AND pace > 0
      AND pace < 60
      AND hole_number IS NOT NULL
),
course_avg AS (
    SELECT
        course_id,
        AVG(pace) AS course_avg_pace
    FROM hole_pace
    GROUP BY course_id
),
hole_stats AS (
    SELECT
        hp.course_id,
        hp.hole_number,
        AVG(hp.pace) AS avg_pace,
        ca.course_avg_pace
    FROM hole_pace hp
    JOIN course_avg ca ON hp.course_id = ca.course_id
    GROUP BY hp.course_id, hp.hole_number, ca.course_avg_pace
)
SELECT
    course_id,
    COUNT(*) AS total_holes,
    SUM(CASE WHEN avg_pace > course_avg_pace * 1.15 THEN 1 ELSE 0 END) AS bottleneck_holes,
    SUM(CASE WHEN avg_pace < course_avg_pace * 0.85 THEN 1 ELSE 0 END) AS fast_holes,
    ROUND(AVG(course_avg_pace), 1) AS course_avg_pace,
    ROUND(MAX(avg_pace), 1) AS slowest_hole_pace,
    ROUND(MIN(avg_pace), 1) AS fastest_hole_pace
FROM hole_stats
GROUP BY course_id
ORDER BY bottleneck_holes DESC
"""

# =============================================================================
# INFRASTRUCTURE & SIZING QUERIES
# =============================================================================

INFRASTRUCTURE_STATS = """
-- Infrastructure statistics for the lakehouse
SELECT
    COUNT(DISTINCT course_id) AS total_courses,
    COUNT(DISTINCT round_id) AS total_rounds,
    COUNT(*) AS total_events,
    SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
    MIN(event_date) AS earliest_date,
    MAX(event_date) AS latest_date,
    COUNT(DISTINCT event_date) AS total_days,
    COUNT(DISTINCT ingest_date) AS ingest_batches
FROM iceberg.silver.fact_telemetry_event
"""

EVENTS_PER_COURSE = """
-- Event counts and date ranges per course
SELECT
    course_id,
    COUNT(DISTINCT round_id) AS rounds,
    COUNT(*) AS total_events,
    SUM(CASE WHEN is_location_padding = FALSE THEN 1 ELSE 0 END) AS real_events,
    MIN(event_date) AS first_date,
    MAX(event_date) AS last_date,
    COUNT(DISTINCT event_date) AS playing_days,
    ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT round_id), 0), 0) AS avg_events_per_round
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY total_events DESC
"""

EVENTS_BY_MONTH = """
-- Events by month for trend analysis
SELECT
    event_year,
    event_month,
    COUNT(DISTINCT course_id) AS courses_active,
    COUNT(DISTINCT round_id) AS rounds,
    COUNT(*) AS events
FROM iceberg.silver.fact_telemetry_event
WHERE event_year IS NOT NULL AND event_month IS NOT NULL
GROUP BY event_year, event_month
ORDER BY event_year, event_month
"""
