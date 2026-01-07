-- Fail if duplicates exist for (round_id, location_index).
-- This is the safer core uniqueness contract for Silver:
-- - fix_timestamp can be NULL (we preserve NULL timestamps)
-- - multiple fixes can share a timestamp, but location_index should still be unique per round

with base as (
  select
    round_id,
    location_index,
    count(*) as cnt
  from {{ source('silver', 'fact_telemetry_event') }}
  group by 1, 2
)
select *
from base
where cnt > 1


