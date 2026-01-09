{% test between_0_and_100(model, column_name) %}
  -- Fails if any values are outside [0, 100].
  --
  -- Use for percentage columns like pct_* and *_rate.
  select *
  from {{ model }}
  where {{ column_name }} is not null
    and ({{ column_name }} < 0 or {{ column_name }} > 100)
{% endtest %}

