# dbt Project

This dbt project builds the **Gold layer** (analytics-ready models) from Silver data.

## What It Does

- **Data Quality Models**: Assess data completeness and quality
- **Analytics Models**: Build summary tables for dashboards and reports
- **Tests**: Validate data quality (duplicates, nulls, etc.)

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

See `docs/learning/dbt_testing_101.md` for more details.
