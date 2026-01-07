with base as (
  select
    course_id,
    round_id,
    count(*) as fix_count,
    sum(case when is_projected then 1 else 0 end) as projected_fix_count,
    sum(case when is_problem then 1 else 0 end) as problem_fix_count
  from {{ source('silver', 'fact_telemetry_event') }}
  where is_location_padding = false
  group by course_id, round_id
)
select
  course_id,
  round_id,
  fix_count,
  projected_fix_count,
  problem_fix_count,
  cast(projected_fix_count as double) / nullif(fix_count, 0) as projected_rate,
  cast(problem_fix_count as double) / nullif(fix_count, 0) as problem_rate
from base


