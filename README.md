## Tagmarshal Lakehouse (Local-first → AWS)

This repo bootstraps a local-first lakehouse for **processed round data** from TagMarshal:

- **Local**: MinIO (S3) + Iceberg (REST catalog) + Spark + Trino + Airflow + dbt
- **AWS**: S3 + Glue Catalog (Iceberg) + Glue Spark + Athena + Airflow + dbt

**Data Sources:**
- CSV files (flattened location columns) — current pilot data
- JSON files (MongoDB export format) — future API integration
- API-ready: switch to live API with a single config change (`TM_DATA_SOURCE=api`)

> **Note:** We're working with **round strings** (pre-processed data), not raw GPS telemetry. TagMarshal's system has already processed raw pings into structured rounds.

The implementation is intentionally **junior-friendly**: clear folder structure, a runbook, and short learning notes.

### Quick start (once implemented)

- Bring the stack up: `just up`
- Upload Bronze (CSV or JSON): `just bronze-upload course_id=<id> input=<path>`
- Run Silver: `just silver course_id=<id> ingest_date=YYYY-MM-DD`
- Run quality gate: `just dq`
- Run Gold: `just gold`

### Switching to API source

When you get API access, update `config/local.env`:
```bash
TM_DATA_SOURCE=api
TM_API_KEY=your-api-key-here
```

### Do I need a Python virtual environment?
No — the pipeline runs in Docker. A local venv is optional if you want to experiment with helper scripts or do Python-only tooling outside containers.

### Key docs

- Local runbook: `docs/runbook_local_dev.md`
- Learning notes: `docs/learning/`
- AWS cutover: `docs/aws_cutover.md`


