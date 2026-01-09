{% test non_negative(model, column_name) %}
  -- Fails if any values are negative.
  --
  -- Use for counts and durations.
  select *
  from {{ model }}
  where {{ column_name }} is not null
    and {{ column_name }} < 0
{% endtest %}

