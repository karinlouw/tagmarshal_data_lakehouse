-- Course Configuration Analysis
-- Analyzes the complexity and variations in course setups
-- Identifies 9-hole, 18-hole, 27-hole courses and shotgun start patterns

{{ config(materialized='table') }}

WITH round_configs AS (
    -- Derive per-round configuration from `gold.fact_rounds` (already filtered to non-padding rows).
    -- This avoids scanning fix-grain telemetry a second time.
    SELECT
        course_id,
        round_id,
        start_hole,
        is_nine_hole,
        is_complete,
        min_section_number AS min_section,
        max_section_number AS max_section,
        holes_played AS unique_holes_played,
        nines_played,
        fix_count AS location_count
    FROM {{ ref('fact_rounds') }}
),

course_summary AS (
    SELECT
        course_id,
        COUNT(DISTINCT round_id) AS total_rounds,
        
        -- Course type indicators
        MAX(max_section) AS max_section_seen,
        MAX(unique_holes_played) AS max_holes_in_round,
        MAX(nines_played) AS max_nines_in_round,
        
        -- Determine course type based on max sections
        CASE 
            WHEN MAX(max_section) > 54 THEN '27-hole'
            WHEN MAX(max_section) > 27 THEN '18-hole'
            ELSE '9-hole'
        END AS likely_course_type,
        
        -- Round type breakdown
        SUM(CASE WHEN is_nine_hole = TRUE THEN 1 ELSE 0 END) AS nine_hole_rounds,
        SUM(CASE WHEN is_nine_hole = FALSE OR is_nine_hole IS NULL THEN 1 ELSE 0 END) AS full_rounds,
        SUM(CASE WHEN is_complete = TRUE THEN 1 ELSE 0 END) AS complete_rounds,
        SUM(CASE WHEN is_complete = FALSE THEN 1 ELSE 0 END) AS incomplete_rounds,
        
        -- Start hole analysis (shotgun starts)
        COUNT(DISTINCT start_hole) AS unique_start_holes,
        SUM(CASE WHEN start_hole = 1 THEN 1 ELSE 0 END) AS rounds_starting_hole_1,
        SUM(CASE WHEN start_hole != 1 AND start_hole IS NOT NULL THEN 1 ELSE 0 END) AS shotgun_start_rounds,
        
        -- Nine analysis (for 27-hole courses)
        SUM(CASE WHEN nines_played = 1 THEN 1 ELSE 0 END) AS single_nine_rounds,
        SUM(CASE WHEN nines_played = 2 THEN 1 ELSE 0 END) AS two_nine_rounds,
        SUM(CASE WHEN nines_played >= 3 THEN 1 ELSE 0 END) AS three_nine_rounds,
        
        -- Average locations per round
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
    
    -- Round completion rates
    ROUND(100.0 * complete_rounds / NULLIF(total_rounds, 0), 1) AS pct_complete,
    ROUND(100.0 * incomplete_rounds / NULLIF(total_rounds, 0), 1) AS pct_incomplete,
    
    -- 9-hole vs full round breakdown
    ROUND(100.0 * nine_hole_rounds / NULLIF(total_rounds, 0), 1) AS pct_nine_hole,
    ROUND(100.0 * full_rounds / NULLIF(total_rounds, 0), 1) AS pct_full_rounds,
    
    -- Shotgun start indicator
    unique_start_holes,
    ROUND(100.0 * shotgun_start_rounds / NULLIF(total_rounds, 0), 1) AS pct_shotgun_starts,
    
    -- For 27-hole courses: which nines are played
    CASE WHEN likely_course_type = '27-hole' THEN
        ROUND(100.0 * single_nine_rounds / NULLIF(total_rounds, 0), 1)
    END AS pct_single_nine,
    CASE WHEN likely_course_type = '27-hole' THEN
        ROUND(100.0 * two_nine_rounds / NULLIF(total_rounds, 0), 1)
    END AS pct_two_nines,
    CASE WHEN likely_course_type = '27-hole' THEN
        ROUND(100.0 * three_nine_rounds / NULLIF(total_rounds, 0), 1)
    END AS pct_all_three_nines,
    
    -- Location density
    avg_locations_per_round,
    min_locations_per_round,
    max_locations_per_round,
    
    -- Complexity score (higher = more complex/varied course)
    ROUND(
        unique_start_holes * 10 +  -- Shotgun starts add complexity
        CASE likely_course_type 
            WHEN '27-hole' THEN 30 
            WHEN '18-hole' THEN 20 
            ELSE 10 
        END +
        CASE WHEN 100.0 * nine_hole_rounds / NULLIF(total_rounds, 0) > 20 THEN 10 ELSE 0 END +  -- Mixed round types
        CASE WHEN 100.0 * incomplete_rounds / NULLIF(total_rounds, 0) > 10 THEN 5 ELSE 0 END   -- Incomplete rounds add tracking difficulty
    , 0) AS course_complexity_score

FROM course_summary
ORDER BY course_complexity_score DESC

