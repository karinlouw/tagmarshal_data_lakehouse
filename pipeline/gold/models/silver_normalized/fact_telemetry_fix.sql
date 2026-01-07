-- fact_telemetry_fix (Silver-normalized)
-- -----------------------------------------------------------------------------
-- Grain: one row per fix/location slot
-- Purpose:
--   Provide a normalized "fact" name for exploration. This intentionally
--   points at the existing Silver long table to avoid duplicating data.
--
-- Why a VIEW?
--   `silver.fact_telemetry_event` is already the correct physical storage for
--   fix-level data. Creating a second Iceberg table would duplicate the entire
--   dataset. A view gives you the normalized naming without extra storage.
-- -----------------------------------------------------------------------------

{{ config(materialized='view') }}

SELECT
    -- Natural key components
    course_id,
    round_id,
    location_index,
    fix_timestamp,

    -- Core flags
    is_timestamp_missing,
    is_location_padding,

    -- Location
    hole_number,
    section_number,
    hole_section,
    nine_number,

    -- Metrics
    pace,
    pace_gap,
    positional_gap,

    -- GPS
    latitude,
    longitude,
    geometry_wkt,

    -- Device + telemetry flags
    battery_percentage,
    is_cache,
    is_projected,
    is_problem,

    -- Round-level repeated fields (kept for convenience)
    ingest_date,
    event_date,
    round_start_time,
    round_end_time,
    start_hole,
    start_section,
    end_section,
    is_nine_hole,
    current_nine,
    goal_time,
    is_complete,
    device AS device_id,
    first_fix,
    last_fix,
    goal_name,
    goal_time_fraction,
    is_incomplete,
    is_secondary,
    is_auto_assigned,
    last_section_start,
    current_section,
    current_hole,
    current_hole_section
FROM {{ source('silver', 'fact_telemetry_event') }}


