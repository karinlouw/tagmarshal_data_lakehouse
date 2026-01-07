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

### `gold.telemetry_completeness_summary`

Per-course completeness summary of the Silver telemetry table. Helps explain how
many rows are CSV padding slots and how often timestamps are missing.

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | STRING | Course identifier |
| `total_rows` | INTEGER | Total Silver rows for this course (including padding) |
| `padding_rows` | INTEGER | Rows where `is_location_padding = TRUE` (CSV padding) |
| `non_padding_rows` | INTEGER | Rows where `is_location_padding = FALSE` (real fixes) |
| `ts_missing_rows` | INTEGER | Rows where `is_timestamp_missing = TRUE` (incl. padding) |
| `ts_missing_non_padding_rows` | INTEGER | Missing timestamps on non-padding rows only |
| `pct_padding_total` | DOUBLE | % of total_rows that are padding_rows |
| `pct_ts_missing_total` | DOUBLE | % of total_rows with missing timestamps |
| `pct_ts_missing_non_padding` | DOUBLE | % of non_padding_rows with missing timestamps |

### `gold.gold_coverage_audit`

Per-course audit table comparing Silver telemetry counts against key Gold models.
This is built to make any unintentional filtering obvious (especially around
padding rows and missing timestamps).

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | STRING | Course identifier |
| `silver_total_rows` | INTEGER | Total Silver rows (including padding) |
| `silver_padding_rows` | INTEGER | Silver rows where `is_location_padding = TRUE` |
| `silver_non_padding_rows` | INTEGER | Silver rows where `is_location_padding = FALSE` |
| `silver_ts_missing_rows` | INTEGER | Silver rows where `is_timestamp_missing = TRUE` |
| `silver_ts_missing_non_padding_rows` | INTEGER | Missing timestamps on non-padding rows |
| `silver_distinct_rounds_all` | INTEGER | Distinct round_ids across all Silver rows |
| `silver_distinct_rounds_non_padding` | INTEGER | Distinct round_ids on non-padding rows only |
| `gold_fact_rounds_rows` | INTEGER | Rows in `gold.fact_rounds` |
| `gold_fact_rounds_distinct_rounds` | INTEGER | Distinct round_ids in `gold.fact_rounds` |
| `gold_fact_rounds_sum_fix_count` | INTEGER | Sum of `fix_count` in `gold.fact_rounds` |
| `gold_hole_perf_rows` | INTEGER | Rows in `gold.fact_round_hole_performance` |
| `gold_hole_perf_distinct_rounds` | INTEGER | Distinct round_ids in hole performance |
| `gold_hole_perf_distinct_round_hole_nine` | INTEGER | Distinct (round_id, hole_number, nine_number) |
| `gold_rounds_by_month_sum_rounds` | INTEGER | Total rounds summed across month buckets |
| `gold_rounds_by_month_unknown_ts_rounds` | INTEGER | Rounds in the “Unknown timestamp” month bucket |
| `gold_rounds_by_weekday_sum_rounds` | INTEGER | Total rounds summed across weekday buckets |
| `gold_rounds_by_weekday_unknown_ts_rounds` | INTEGER | Rounds in the “Unknown timestamp” weekday bucket |
| `gold_dim_course_present` | INTEGER | 1 if course exists in `gold.dim_course` |
| `unit_count` | INTEGER | Count of topology units (if available) |

## Build Command

```bash
just gold
```

## Source

- Input: Silver layer Iceberg table
- Transform: dbt models in `pipeline/gold/models/`
- Format: Iceberg tables via Trino

