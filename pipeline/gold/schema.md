# Gold Layer Schema

Analytics-ready aggregated views built with dbt.

## Tables

### `gold.pace_summary_by_round`

Round-level pace metrics for performance analysis.

| Column | Type | Description |
|--------|------|-------------|
| `round_id` | STRING | Unique round identifier |
| `course_id` | STRING | Course identifier |
| `event_date` | DATE | Round date |
| `avg_pace` | DOUBLE | Average pace (seconds behind goal) |
| `max_pace` | DOUBLE | Worst pace in round |
| `total_fixes` | INTEGER | Number of GPS fixes |
| `is_complete` | BOOLEAN | Round completion status |

### `gold.device_health_errors`

Device issues and error tracking.

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | STRING | Course identifier |
| `event_date` | DATE | Error date |
| `error_type` | STRING | Type of error (low_battery, offline, etc.) |
| `error_count` | INTEGER | Number of occurrences |
| `affected_rounds` | INTEGER | Rounds with this error |

### `gold.signal_quality_rounds`

GPS signal quality metrics.

| Column | Type | Description |
|--------|------|-------------|
| `round_id` | STRING | Unique round identifier |
| `course_id` | STRING | Course identifier |
| `missing_gps_pct` | DOUBLE | % of fixes with no GPS |
| `projected_pct` | DOUBLE | % of projected locations |
| `signal_quality` | STRING | Good/Fair/Poor rating |

### `gold.data_quality_overview`

High-level data quality summary.

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | STRING | Course identifier |
| `total_rounds` | INTEGER | Total round count |
| `total_events` | INTEGER | Total GPS fix count |
| `completeness_score` | DOUBLE | Overall data quality % |

### `gold.critical_column_gaps`

Missing data analysis by column.

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | STRING | Course identifier |
| `column_name` | STRING | Column being analysed |
| `null_count` | INTEGER | Number of NULL values |
| `null_pct` | DOUBLE | Percentage NULL |
| `status` | STRING | Good/Warning/Critical |

### `gold.course_configuration_analysis`

Course setup and configuration patterns.

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | STRING | Course identifier |
| `hole_count` | INTEGER | Number of holes (9/18/27) |
| `has_shotgun` | BOOLEAN | Shotgun starts detected |
| `pct_nine_hole` | DOUBLE | % of 9-hole rounds |

## Build Command

```bash
just gold
```

## Source

- Input: Silver layer Iceberg table
- Transform: dbt models in `pipeline/gold/models/`
- Format: Iceberg tables via Trino

