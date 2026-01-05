-- Fail if duplicates exist for (round_id, fix_timestamp).
-- This is our core dedup contract for Silver.

with base as (
  select
    round_id,
    fix_timestamp,
    count(*) as cnt
  from {{ source('silver', 'fact_telemetry_event') }}
  group by 1, 2
)
select *
from base
where cnt > 1


