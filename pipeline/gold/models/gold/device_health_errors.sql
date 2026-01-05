with base as (
  select
    course_id,
    round_id,
    fix_timestamp,
    battery_percentage
  from {{ source('silver', 'fact_telemetry_event') }}
)
select
  course_id,
  round_id,
  fix_timestamp,
  battery_percentage,
  case
    when battery_percentage is null then null
    when battery_percentage < 10 then 'battery_critical'
    when battery_percentage < 20 then 'battery_low'
    else null
  end as health_flag
from base
where battery_percentage is not null and battery_percentage < 20


