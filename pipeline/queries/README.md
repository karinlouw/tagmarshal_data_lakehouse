# SQL Queries

This directory contains SQL queries for data exploration and analysis.

## Structure

- **`exploration/`** - Ad-hoc exploration queries used in dashboards and analysis
- **`examples/`** - Example queries for documentation and learning

## Usage

### In Python/Streamlit

```python
with open('queries/exploration/executive_summary.sql', 'r') as f:
    query = f.read()
```

### In DBeaver/Trino

Simply open and run any `.sql` file directly.

## Query Catalog

### Exploration Queries

| Query | Description | Used In |
|-------|-------------|---------|
| `executive_summary.sql` | Total courses, rounds, events | Dashboard |
| `data_quality_overview.sql` | Quality scores by course | Dashboard |
| `critical_gaps.sql` | Missing data analysis | Dashboard |
| `course_configuration.sql` | Course type and complexity | Dashboard |
| `battery_analysis.sql` | Device battery health | Dashboard |
| `pace_gap_coverage.sql` | Missing pace_gap values | Dashboard |
| `dataset_variance.sql` | Data volume comparison | Dashboard |
| `null_analysis.sql` | Detailed null breakdown | Dashboard |
| `column_completeness.sql` | Column completeness heatmap | Dashboard |

## Adding New Queries

1. Create a new `.sql` file in the appropriate subdirectory
2. Add a descriptive comment at the top explaining what it does
3. Update this README with the new query
4. If used in dashboard, update `dashboard/app.py` to load it

## Best Practices

- Use descriptive file names (snake_case)
- Include comments explaining the query purpose
- Order results logically (most important first)
- Use consistent formatting and style

