-- Tagmarshal Lakehouse Demo: Silver + Gold Insights (DBeaver/Trino)
--
-- This file is intended to be run top-to-bottom in DBeaver (Trino connector).
-- All queries use fully-qualified Iceberg schema names.
--
-- Prereqs:
--   - Silver built:  just silver-all   (or just silver ...)
--   - Topology seeded: just topology-refresh
--   - Gold built:    just gold
--   - Course profile seeded: just seed-course-profile
--
-- Tip: You can change course IDs / hole numbers inline to demo other facilities.

-- ============================================================================
-- 0) Quick sanity checks
-- ============================================================================

-- 0a) Data volume by course (rounds + fixes). Confirms Silver is populated.
SELECT
  course_id,
  COUNT(DISTINCT round_id) AS rounds,
  COUNT(*) AS fixes
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id
ORDER BY fixes DESC;

-- 0b) Topology rows by facility. Confirms topology table is seeded.
SELECT
  facility_id,
  COUNT(*) AS topology_rows
FROM iceberg.silver.dim_facility_topology
GROUP BY facility_id
ORDER BY facility_id;

-- 0d) Seasonality (Gold): rounds per month (month name included).
-- Uses round start time inferred from Silver (MIN(fix_timestamp) per round_id).
SELECT
  course_id,
  month_start,
  month_name,
  rounds,
  pct_total
FROM iceberg.gold.course_rounds_by_month
ORDER BY course_id, month_start;

-- 0d.1) Top 3 months per course (by share of total rounds).
WITH ranked AS (
  SELECT
    course_id,
    month_start,
    month_name,
    rounds,
    pct_total,
    row_number() OVER (PARTITION BY course_id ORDER BY pct_total DESC, month_start) AS rn
  FROM iceberg.gold.course_rounds_by_month
)
SELECT
  course_id,
  month_name,
  month_start,
  rounds,
  pct_total
FROM ranked
WHERE rn <= 3
ORDER BY course_id, rn;

-- 0d.2) Bottom 3 months per course (by share of total rounds).
WITH ranked AS (
  SELECT
    course_id,
    month_start,
    month_name,
    rounds,
    pct_total,
    row_number() OVER (PARTITION BY course_id ORDER BY pct_total ASC, month_start) AS rn
  FROM iceberg.gold.course_rounds_by_month
)
SELECT
  course_id,
  month_name,
  month_start,
  rounds,
  pct_total
FROM ranked
WHERE rn <= 3
ORDER BY course_id, rn;

-- 0d.3) Single Course Seasonality Deep Dive
-- Example: Bradshaw Farm GC

-- All months for single course:
SELECT
  month_name,
  rounds,
  pct_total
FROM iceberg.gold.course_rounds_by_month
WHERE course_id = 'bradshawfarmgc'
ORDER BY month_start;

-- Top 3 months for single course:
SELECT
  month_name,
  rounds,
  pct_total
FROM iceberg.gold.course_rounds_by_month
WHERE course_id = 'bradshawfarmgc'
ORDER BY pct_total DESC
LIMIT 3;

-- Bottom 3 months for single course:
SELECT
  month_name,
  rounds,
  pct_total
FROM iceberg.gold.course_rounds_by_month
WHERE course_id = 'bradshawfarmgc'
ORDER BY pct_total ASC
LIMIT 3;

-- 0e) Seasonality (Gold): rounds by weekday (weekday name included).
SELECT
  course_id,
  weekday_number,
  weekday_name,
  rounds
FROM iceberg.gold.course_rounds_by_weekday
ORDER BY course_id, weekday_number;

-- ============================================================================
-- 1) Bradshaw Farm (27-hole units): topology + unit breakdown
-- ============================================================================

-- 1a) Bradshaw topology rows (units + section ranges).
-- Expected: 3 rows for a 27-hole facility (Unit 1/2/3), with non-overlapping section ranges.
SELECT
  facility_id,
  unit_id,
  unit_name,
  nine_number,
  section_start,
  section_end
FROM iceberg.silver.dim_facility_topology
WHERE facility_id = 'bradshawfarmgc'
ORDER BY nine_number;

-- 1b) Bradshaw unit summary (Silver, event-level).
-- Shows volume + average pace metrics by unit (`nine_number`).
SELECT
  nine_number,
  COUNT(DISTINCT round_id) AS rounds,
  COUNT(*) AS fixes,
  AVG(pace) AS avg_pace_sec,
  AVG(pace_gap) AS avg_pace_gap_sec
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'bradshawfarmgc'
  AND nine_number IS NOT NULL
GROUP BY nine_number
ORDER BY nine_number;

-- ============================================================================
-- 2) American Falls (9-hole loop): loop fatigue on same physical hole
-- ============================================================================

-- 2a.1) American Falls (Silver): event-level pace by loop for a single hole (ALL rounds).
-- This includes BOTH 9-hole rounds and 18-hole rounds (i.e., it will include loop-1-only rounds).
-- Expected: often 1 row (only nine_number=1) if many rounds end after 9, or 2 rows if many rounds include loop 2.
-- Change hole_number to demo other holes.
SELECT
  nine_number,
  AVG(pace) AS avg_pace_sec,
  COUNT(*) AS fixes
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'americanfalls'
  AND hole_number = 5
GROUP BY nine_number
ORDER BY nine_number;

-- 2a.2) American Falls (Silver): event-level pace by loop for a single hole (18-hole rounds only).
-- We define “18-hole” here as: the round_id contains BOTH nine_number=1 and nine_number=2 somewhere in telemetry.
WITH eligible_rounds AS (
  SELECT
    round_id
  FROM iceberg.silver.fact_telemetry_event
  WHERE course_id = 'americanfalls'
    AND nine_number IN (1, 2)
    AND hole_number IS NOT NULL
    AND fix_timestamp IS NOT NULL
  GROUP BY round_id
  HAVING COUNT(DISTINCT nine_number) = 2
)
SELECT
  e.nine_number,
  AVG(e.pace) AS avg_pace_sec,
  COUNT(*) AS fixes
FROM iceberg.silver.fact_telemetry_event e
JOIN eligible_rounds r
  ON e.round_id = r.round_id
WHERE e.course_id = 'americanfalls'
  AND e.hole_number = 5
GROUP BY e.nine_number
ORDER BY e.nine_number;

-- 2a.3) American Falls (Silver): event-level pace by loop for a single hole (18-hole + completed rounds only).
-- This is the strictest “fatigue” view: only rounds that reached loop 2 AND were marked complete.
WITH eligible_rounds AS (
  SELECT
    round_id
  FROM iceberg.silver.fact_telemetry_event
  WHERE course_id = 'americanfalls'
    AND nine_number IN (1, 2)
    AND hole_number IS NOT NULL
    AND fix_timestamp IS NOT NULL
  GROUP BY round_id
  HAVING COUNT(DISTINCT nine_number) = 2
)
SELECT
  e.nine_number,
  AVG(e.pace) AS avg_pace_sec,
  COUNT(*) AS fixes
FROM iceberg.silver.fact_telemetry_event e
JOIN eligible_rounds r
  ON e.round_id = r.round_id
WHERE e.course_id = 'americanfalls'
  AND e.hole_number = 5
  AND e.is_complete = true
GROUP BY e.nine_number
ORDER BY e.nine_number;

-- 2b) American Falls (Gold): hole-level aggregation by loop for the same hole.
-- This is the demo-friendly “Fatigue Factor” view (aggregated per hole instance).
SELECT
  nine_number,
  AVG(avg_pace_sec) AS avg_pace_sec,
  COUNT(*) AS hole_instances
FROM iceberg.gold.fact_round_hole_performance
WHERE course_id = 'americanfalls'
  AND hole_number = 5
GROUP BY nine_number
ORDER BY nine_number;

-- ============================================================================
-- 3) Indian Creek: shotgun/random start validation
-- ============================================================================

-- 3) Start hole distribution (shotgun/random start signal).
-- Expected: more than one start_hole shows up with non-trivial counts.
SELECT
  start_hole,
  COUNT(DISTINCT round_id) AS rounds
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'indiancreek'
  AND start_hole IS NOT NULL
GROUP BY start_hole
ORDER BY rounds DESC, start_hole;

-- ============================================================================
-- 4) Frequency checks for topology boundaries (outlier visibility)
-- ============================================================================
-- Goal: show whether section-number "edges" are supported by substantial frequency.
--
-- This is a data quality demonstration. A single rare section number can appear due to
-- GPS/geofence anomalies; we want to see if boundary sections are common or outliers.

-- 4a) Rarest section numbers across all courses (potential outliers).
-- Use this to spot “singletons” that could distort naive min/max logic.
SELECT
  course_id,
  section_number,
  COUNT(*) AS fixes
FROM iceberg.silver.fact_telemetry_event
WHERE section_number IS NOT NULL
GROUP BY course_id, section_number
ORDER BY fixes ASC
LIMIT 50;

-- 4b) Boundary support check (Bradshaw):
-- Counts how many fixes land exactly on each section_start/section_end for every unit.
-- Low counts at boundaries can signal noisy edges (or low volume).
WITH topo AS (
  SELECT facility_id, nine_number, section_start, section_end
  FROM iceberg.silver.dim_facility_topology
  WHERE facility_id = 'bradshawfarmgc'
),
counts AS (
  SELECT course_id, section_number, COUNT(*) AS fixes
  FROM iceberg.silver.fact_telemetry_event
  WHERE course_id = 'bradshawfarmgc'
    AND section_number IS NOT NULL
  GROUP BY course_id, section_number
)
SELECT
  t.facility_id AS course_id,
  t.nine_number,
  t.section_start,
  COALESCE(c1.fixes, 0) AS fixes_at_section_start,
  t.section_end,
  COALESCE(c2.fixes, 0) AS fixes_at_section_end
FROM topo t
LEFT JOIN counts c1
  ON c1.course_id = t.facility_id AND c1.section_number = t.section_start
LEFT JOIN counts c2
  ON c2.course_id = t.facility_id AND c2.section_number = t.section_end
ORDER BY t.nine_number;

-- 4c) Outlier test per hole (Bradshaw):
-- For each hole_number, take the MAX(section_number) and show how many fixes hit that max.
-- If fixes_at_max_section is tiny, that “max” is likely an outlier.
WITH hole_section_counts AS (
  SELECT
    course_id,
    hole_number,
    section_number,
    COUNT(*) AS fixes
  FROM iceberg.silver.fact_telemetry_event
  WHERE course_id = 'bradshawfarmgc'
    AND hole_number IS NOT NULL
    AND section_number IS NOT NULL
  GROUP BY 1,2,3
),
max_per_hole AS (
  SELECT
    course_id,
    hole_number,
    MAX(section_number) AS max_section
  FROM hole_section_counts
  GROUP BY 1,2
)
SELECT
  m.course_id,
  m.hole_number,
  m.max_section,
  c.fixes AS fixes_at_max_section
FROM max_per_hole m
JOIN hole_section_counts c
  ON m.course_id = c.course_id
 AND m.hole_number = c.hole_number
 AND m.max_section = c.section_number
ORDER BY fixes_at_max_section ASC, m.hole_number;
