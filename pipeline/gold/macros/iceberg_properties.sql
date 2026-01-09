{% macro tm_iceberg_partitioning_course_id() -%}
  {#-
    Trino Iceberg supports table partitioning via table properties.

    We standardize on partitioning by `course_id` for Gold rollups because:
    - Most analysis filters by course_id
    - It improves partition pruning without over-complicating the design

    This macro returns a dict suitable for dbt-trino's `properties=` model config.
  -#}
  {{ return({"partitioning": "ARRAY['course_id']"}) }}
{%- endmacro %}

{% macro tm_iceberg_partitioning_course_id_post_hook() -%}
  {#-
    dbt-trino does not reliably apply Iceberg `partitioning` via CREATE TABLE properties
    across versions/adapters.

    Trino *does* support:
      ALTER TABLE <table> SET PROPERTIES partitioning = ARRAY['course_id']

    We apply partitioning via a post-hook so it is enforced after dbt creates the table.
  -#}
  {{ return(["ALTER TABLE " ~ this ~ " SET PROPERTIES partitioning = ARRAY['course_id']"]) }}
{%- endmacro %}

