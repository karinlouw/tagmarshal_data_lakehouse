# Silver Layer Schema

Cleaned, transformed, conformed source of truth.

## Table: `silver.fact_telemetry_event`

One row per GPS fix (location reading) from a device during a round.

# Todo: We should add a schema to the docs
## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `round_id` | STRING | NO | Unique round identifier |
| `course_id` | STRING | NO | Course identifier |
| `ingest_date` | DATE | NO | Date data was ingested |
| `fix_timestamp` | TIMESTAMP | YES | GPS fix timestamp |
| `is_timestamp_missing` | BOOLEAN | NO | Flag for NULL timestamps |
| `event_date` | DATE | YES | Date from fix_timestamp |
| `location_index` | INTEGER | NO | Position in location array |
| `hole_number` | INTEGER | YES | Hole number (1-27) |
| `section_number` | INTEGER | YES | Cumulative section (1-117) |
| `hole_section` | INTEGER | YES | Section within hole |
| `nine_number` | INTEGER | YES | Which 9-hole set (1-3) |
| `pace` | DOUBLE | YES | Seconds behind goal |
| `pace_gap` | DOUBLE | YES | Gap to group ahead |
| `positional_gap` | DOUBLE | YES | Position vs group ahead |
| `latitude` | DOUBLE | YES | GPS latitude |
| `longitude` | DOUBLE | YES | GPS longitude |
| `geometry_wkt` | STRING | YES | WKT point format |
| `battery_percentage` | DOUBLE | YES | Device battery (0-100) |
| `start_hole` | INTEGER | YES | Round start hole |
| `goal_time` | INTEGER | YES | Target time (seconds) |
| `is_cache` | BOOLEAN | YES | Offline data flag |
| `is_projected` | BOOLEAN | YES | Estimated position flag |
| `is_problem` | BOOLEAN | YES | Problem group flag |

## Partitioning

- **Partition by:** `course_id`, `ingest_date`
# Todo: What do you mean cluster by'?
- **Cluster by:** `round_id`, `fix_timestamp`

## Transformations Applied

1. **Explode locations** - One row per location array element
2. **Deduplicate** - Remove duplicate (round_id, fix_timestamp) pairs # Todo: are we sure these are unique?
3. **Round decimals** - Pace values to 3 decimal places
4. **Derive fields** - `nine_number`, `geometry_wkt`, `event_date` # Todo: We need to double check nine_number logic
5. **Filter empties** - Remove rows where hole AND section are NULL # Todo: Double check we removed this

## Data Quality Flags

# Todo: We need othe dq flags like pace?

| Flag | Purpose |
|------|---------|
| `is_timestamp_missing` | Track records with NULL source timestamp |
| `is_cache` | Identify offline-uploaded data |
| `is_projected` | Identify estimated positions |

## Related Commands

```bash
just silver course_id=bradshawfarmgc ingest_date=2025-06-28
```

## Source

- Input: Bronze layer CSV/JSON files
- ETL: `pipeline/silver/etl.py`
- Format: Apache Iceberg table

