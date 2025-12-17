## Tagmarshal Lakehouse (Local-first → AWS)

This repo bootstraps a local-first lakehouse for geospatial telemetry:

- **Local**: MinIO (S3) + Iceberg (REST catalog) + Spark/Sedona + Trino + Airflow + dbt
- **AWS**: S3 + Glue Catalog (Iceberg) + Glue Spark + Athena + Airflow triggers + dbt (Athena)

The implementation is intentionally **junior-friendly**: clear folder structure, a runbook, and short learning notes.

### Quick start (once implemented)

- Bring the stack up: `just up`
- Upload Bronze: `just bronze-upload course_id=<id> input=<path>`
- Run Silver: `just silver course_id=<id> ingest_date=YYYY-MM-DD`
- Run quality gate: `just dq`
- Run Gold: `just gold`

### Do I need a Python virtual environment?
No — the pipeline runs in Docker. A local venv is optional if you want to experiment with helper scripts or do Python-only tooling outside containers.

### Key docs

- Local runbook: `docs/runbook_local_dev.md`
- Learning notes: `docs/learning/`
- AWS cutover: `docs/aws_cutover.md`


