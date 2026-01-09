-- pace_summary_by_round.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per (course_id, round_id)
-- Purpose:
--   Lightweight pace summary for dashboards. This is intentionally derived from
--   `gold.fact_rounds` to avoid re-scanning fix-grain telemetry.
-- -----------------------------------------------------------------------------

{{ config(materialized='table') }}

SELECT
  course_id,
  round_id,
  round_start_ts,
  round_end_ts,
  fix_count,
  avg_pace_sec           AS avg_pace,
  avg_pace_gap_sec       AS avg_pace_gap,
  avg_positional_gap     AS avg_positional_gap
FROM {{ ref('fact_rounds') }}


