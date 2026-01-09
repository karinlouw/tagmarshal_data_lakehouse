{% macro apply_gold_course_partitioning(models) -%}
  {#-
    Ensure key Gold tables are physically partitioned in Iceberg.

    Why this exists:
    - Trino supports Iceberg partitioning via: ALTER TABLE ... SET PROPERTIES partitioning = ARRAY['col']
    - Applying partitioning via CREATE TABLE properties / adapter configs can be inconsistent across dbt-trino versions

    Usage (CLI):
      dbt run-operation apply_gold_course_partitioning --args '{"models": ["fact_rounds", ...]}'
  -#}

  {% if models is none %}
    {{ exceptions.raise_compiler_error("apply_gold_course_partitioning: missing required arg `models`") }}
  {% endif %}

  {% for model_name in models %}
    {% set rel = adapter.get_relation(database=target.catalog, schema='gold', identifier=model_name) %}
    {% if rel is none %}
      {{ log("apply_gold_course_partitioning: skipping missing relation gold." ~ model_name, info=true) }}
    {% else %}
      {{ log("apply_gold_course_partitioning: partitioning " ~ rel, info=true) }}
      {% do run_query("ALTER TABLE " ~ rel ~ " SET PROPERTIES partitioning = ARRAY['course_id']") %}
    {% endif %}
  {% endfor %}
{%- endmacro %}

