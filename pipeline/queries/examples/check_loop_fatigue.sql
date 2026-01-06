-- Query to verify "Fatigue Factor" by comparing performance on the same hole
-- across different loops (nines).
--
-- Example: American Falls (9-hole course played twice)
-- We expect to see two rows for Hole 5:
--   nine_number = 1 (First Loop)
--   nine_number = 2 (Second Loop)

SELECT 
    nine_number, 
    AVG(avg_pace_sec) as avg_pace_seconds,
    count(*) as rounds_sample_size
FROM iceberg.gold.fact_round_hole_performance
WHERE course_id = 'americanfalls' 
  AND hole_number = 5
GROUP BY nine_number
ORDER BY nine_number;

