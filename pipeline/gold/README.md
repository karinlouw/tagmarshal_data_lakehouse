# dbt Project

This dbt project builds the **Gold layer** (analytics-ready models) from Silver data.

## What It Does

- **Data Quality Models**: Assess data completeness and quality
- **Analytics Models**: Build summary tables for dashboards and reports
- **Tests**: Validate data quality (duplicates, nulls, etc.)
- **Docs**: Generate dbt docs (catalog + lineage) for junior-friendly discovery

## Models

All models are in `models/gold/`:
- `data_quality_overview.sql` - High-level quality metrics
- `critical_column_gaps.sql` - Missing data analysis
- `course_configuration_analysis.sql` - Course setup analysis
- And more...

## Running dbt

```bash
# Build all Gold models
just gold

# Run specific model
just dbt-run model_name
```

## Tests (strict)

The Gold build runs `dbt test` as part of the `gold_dbt` Airflow DAG. If any tests fail, the DAG run fails.

## Docs

The Gold build generates dbt docs and uploads them to the observability bucket.
After a run, look for a link in the Gold DAG logs, or browse in MinIO under:

- `tm-lakehouse-observability/gold/` (per-run prefix)

See `docs/learning/dbt_testing_101.md` for more details.
