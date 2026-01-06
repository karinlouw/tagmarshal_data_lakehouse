-- Shotgun start validation
-- Indian Creek is expected to have rounds that start on many different holes (not just hole 1).
-- This query shows the distribution of start holes for the course.

SELECT
  start_hole,
  COUNT(DISTINCT round_id) AS rounds
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'indiancreek'
  AND start_hole IS NOT NULL
GROUP BY start_hole
ORDER BY rounds DESC, start_hole;


