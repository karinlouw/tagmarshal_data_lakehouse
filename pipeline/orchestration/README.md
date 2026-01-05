# Airflow Orchestration

This directory contains Airflow configuration and DAGs for orchestrating the data pipeline.

## Structure

- **`dags/`** - Airflow DAG definitions:
  - `bronze_ingest_dag.py` - Ingests CSV/JSON files to Bronze layer
  - `silver_etl_dag.py` - Transforms Bronze to Silver layer
  - `gold_dbt_dag.py` - Runs dbt models to build Gold layer

- **`entrypoint.sh`** - Container startup script

## Running Airflow

Airflow runs in Docker. Start it with:

```bash
just up
```

Then access the UI at http://localhost:8080 (default credentials: airflow/airflow)

## DAGs

Each DAG handles one layer of the medallion architecture:
- **Bronze**: Raw data ingestion
- **Silver**: Cleaned, transformed data
- **Gold**: Analytics-ready summaries

See `docs/learning/airflow_101.md` for more details.
