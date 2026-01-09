-- signal_quality_rounds.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per (course_id, round_id)
-- Purpose:
--   Round-level signal quality rates (projected / problem). Derived from
--   `gold.fact_rounds` to avoid re-scanning fix-grain telemetry.
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

SELECT
  course_id,
  round_id,
  fix_count,
  projected_fix_count,
  problem_fix_count,
  projected_rate,
  problem_rate
FROM {{ ref('fact_rounds') }}


