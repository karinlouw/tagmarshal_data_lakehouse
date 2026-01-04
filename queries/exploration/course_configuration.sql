-- Course Configuration Analysis
-- Analyzes course types, hole counts, and start patterns

SELECT 
    course_id,
    total_rounds,
    likely_course_type,
    max_section_seen,
    max_holes_in_round,
    ROUND(pct_nine_hole, 1) as pct_nine_hole,
    unique_start_holes,
    ROUND(pct_shotgun_starts, 1) as pct_shotgun_starts,
    course_complexity_score
FROM iceberg.gold.course_configuration_analysis
ORDER BY course_complexity_score DESC

