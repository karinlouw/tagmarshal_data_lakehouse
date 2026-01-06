with base as (
  select
    course_id,
    round_id,
    min(fix_timestamp) as round_start_ts,
    max(fix_timestamp) as round_end_ts,
    count(*) as fix_count,
    avg(pace) as avg_pace,
    avg(pace_gap) as avg_pace_gap,
    avg(positional_gap) as avg_positional_gap
  from {{ source('silver', 'fact_telemetry_event') }}
  group by course_id, round_id
)
select
  course_id,
  round_id,
  round_start_ts,
  round_end_ts,
  fix_count,
  avg_pace,
  avg_pace_gap,
  avg_positional_gap
from base


