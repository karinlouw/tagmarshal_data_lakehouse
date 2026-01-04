# Spark ETL Jobs

This directory contains PySpark jobs that transform data between Bronze and Silver layers.

## Files

- **`silver_etl.py`** - Main Silver ETL job that:
  - Reads CSV or JSON files from Bronze (Landing Zone)
  - Transforms wide CSV or nested JSON into long-format (one row per GPS fix)
  - Writes to Silver Iceberg table (`silver.fact_telemetry_event`)

## How It Works

1. **Detects file format** (CSV or JSON) automatically
2. **Reads data** from MinIO/S3 Bronze bucket
3. **Explodes location arrays** into individual rows
4. **Adds derived fields** (timestamps, nine_number, etc.)
5. **Deduplicates** using round_id + fix_timestamp
6. **Writes to Iceberg** table in Silver layer

## Running Locally

Jobs are run via Airflow DAGs, but you can test manually:

```bash
# Run Silver ETL for a course
just silver course_id=<course_id> ingest_date=YYYY-MM-DD
```

See `docs/learning/pipeline_walkthrough.md` for detailed examples.
