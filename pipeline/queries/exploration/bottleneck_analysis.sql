-- Bottleneck Analysis by Section
-- Identifies where groups bunch up using pace_gap (time to group ahead)
--
-- Why pace_gap is better than pace:
--   - pace = seconds behind goal time (arbitrary, set by course)
--   - pace_gap = actual spacing to group ahead (real-world bottleneck indicator)
--   - High/variable pace_gap = groups bunching up = bottleneck
--
-- Key columns:
--   pace_gap - Time gap to group ahead (seconds). Spike = bottleneck forming
--   positional_gap - Position relative to group ahead. Positive = falling behind
--   section_number - Cumulative section (1-57 for 18 holes)
--   hole_number - Which hole (1-18)

SELECT 
    course_id,
    hole_number,
    section_number,
    hole_section,
    ROUND(AVG(latitude), 5) as lat,
    ROUND(AVG(longitude), 5) as lon,
    -- Pace gap metrics (primary bottleneck indicator)
    ROUND(AVG(pace_gap), 0) as avg_pace_gap_seconds,
    ROUND(STDDEV(pace_gap), 0) as pace_gap_stddev,
    -- Positional gap (secondary indicator)
    ROUND(AVG(positional_gap), 0) as avg_positional_gap,
    -- Traditional pace for reference only
    ROUND(AVG(pace), 0) as avg_pace_seconds,
    -- Volume metrics
    COUNT(DISTINCT round_id) as rounds_measured,
    COUNT(*) as total_fixes
FROM iceberg.silver.fact_telemetry_event 
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL 
  AND pace_gap IS NOT NULL
  AND hole_number IS NOT NULL
GROUP BY course_id, hole_number, section_number, hole_section
HAVING COUNT(*) > 50
ORDER BY course_id, section_number
