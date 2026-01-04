# Silver Layer: fact_telemetry_event

Main fact table for telemetry events (one row per GPS fix).

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `round_id` | STRING | NO | Unique round identifier |
| `course_id` | STRING | NO | Course identifier |
| `fix_timestamp` | TIMESTAMP | NO | GPS fix timestamp |
| `location_index` | INTEGER | NO | Position in round's location array |
| `pace` | DOUBLE | YES | Pace value (seconds behind goal) |
| `pace_gap` | DOUBLE | YES | Pace gap between consecutive fixes |
| `positional_gap` | DOUBLE | YES | Positional gap (distance) |
| `hole_number` | INTEGER | YES | Hole number (1-18, or 1-27 for 27-hole courses) |
| `section_number` | INTEGER | YES | Section number (1-117) |
| `nine_number` | INTEGER | YES | Which 9-hole set (1, 2, or 3 for 27-hole courses) |
| `latitude` | DOUBLE | YES | GPS latitude |
| `longitude` | DOUBLE | YES | GPS longitude |
| `battery_percentage` | INTEGER | YES | Device battery level (0-100) |
| `start_hole` | INTEGER | YES | Starting hole for the round |
| `goal_time` | INTEGER | YES | Goal time in seconds |
| `is_cache` | BOOLEAN | YES | Whether this is cached data |
| `ingest_date` | DATE | NO | Date when data was ingested |
| `event_date` | DATE | NO | Date of the event (derived from fix_timestamp) |

## Partitioning

- Partitioned by: `course_id`, `ingest_date`
- Clustered by: `round_id`, `fix_timestamp`

## Data Quality

- **Deduplication**: One row per `(round_id, fix_timestamp)` combination
- **Null Handling**: Empty location slots are filtered out (hole_number and section_number both NULL)

## Source

Created by `jobs/spark/silver_etl.py` from Bronze layer CSV/JSON files.

