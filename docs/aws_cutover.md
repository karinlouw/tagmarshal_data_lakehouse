## AWS cutover (S3 + Glue Catalog + Glue Spark + Athena + Airflow)

This project is designed so local ↔ AWS is **config-only** (swap env vars / job parameters), not a rewrite.

### 1) Create S3 buckets
Create (or pick) these buckets in your target region:
- `tm-lakehouse-landing-zone` - Raw data landing zone
- `tm-lakehouse-source-store` - Silver: cleaned, conformed data
- `tm-lakehouse-serve` - Gold: analytics-ready data
- `tm-lakehouse-quarantine` - Invalid data
- `tm-lakehouse-observability` - Run logs and artifacts

Keep the same object layout:
- Landing zone keys: `course_id=<id>/ingest_date=YYYY-MM-DD/<filename>`
- Silver Iceberg tables: `warehouse/silver/...`
- Gold Iceberg tables: `warehouse/gold/...`

### 2) Glue Data Catalog (Iceberg)
Create Glue databases (namespaces) to match local:
- `silver`
- `gold`

Set these runtime vars for Spark jobs:
- `TM_ENV=aws`
- `TM_ICEBERG_CATALOG_TYPE=glue`
- `TM_ICEBERG_WAREHOUSE_SILVER=s3://tm-lakehouse-source-store/warehouse`
- `TM_ICEBERG_WAREHOUSE_GOLD=s3://tm-lakehouse-serve/warehouse`
- `TM_DB_SILVER=silver`, `TM_DB_GOLD=gold`

Authentication:
- Prefer **IAM role** on Glue (do not pass access keys).

### 3) Glue Spark job packaging (Silver ETL)
Package and upload your Spark job code to S3 (one of these patterns):
- **Pattern A (simple)**: upload the single script `jobs/spark/silver_etl.py` to S3 and point the Glue job to it.
- **Pattern B (recommended)**: zip your `jobs/` folder (including `jobs/spark/lib/tm_lakehouse`) and pass it via `--extra-py-files` so imports work.

Glue Job parameters (example):
- `--course-id <course_id>`
- `--ingest-date YYYY-MM-DD`
- `--bronze-prefix course_id=<id>/ingest_date=YYYY-MM-DD/`
- Env vars (Glue job parameters or job configuration):
  - `TM_BUCKET_LANDING`, `TM_BUCKET_SOURCE`, `TM_BUCKET_SERVE`, `TM_BUCKET_QUARANTINE`, `TM_BUCKET_OBSERVABILITY`
  - `TM_ICEBERG_CATALOG_TYPE=glue`
  - `TM_ICEBERG_WAREHOUSE_SILVER=s3://tm-lakehouse-source-store/warehouse`

Dependencies:
- Iceberg is supported in modern Glue versions; validate your Glue version supports the Iceberg Spark extensions.
- If you add Sedona/H3 later, package jars/wheels accordingly (start without it).

### 4) Athena
Athena will query your Iceberg tables via Glue Catalog.
- Create/choose an Athena workgroup.
- Configure query results location (S3).

### 5) Airflow in AWS (MWAA or ECS/EKS)
Airflow becomes the "control plane":
- Trigger Glue jobs for Silver ETL
- Run dbt for Gold + data quality (against Athena)

Airflow operators you'll likely use:
- `AwsGlueJobOperator` to start and monitor Glue ETL jobs
- `BashOperator` (or custom operator) to run dbt in a container/task

### 6) dbt on Athena (data quality + Gold)
Switch to the Athena adapter:
- `dbt-athena-community`

Typical profile fields you'll set:
- `region_name`
- `s3_staging_dir`
- `work_group`
- `database` (Glue database, e.g. `silver`/`gold`)

Artifacts + observability:
- Upload `manifest.json` and `run_results.json` to `tm-lakehouse-observability/dbt_artifacts/...`
- Continue writing run summaries and quarantine reports to S3 (same prefixes as local).

### 7) IAM checklist (minimum)
Glue job role:
- Read from Landing Zone bucket/prefix
- Write to Source Store bucket/warehouse prefix
- Write to Serve bucket/warehouse prefix
- Read/Write to Quarantine + Observability buckets
- Glue Catalog permissions for Iceberg table operations

Athena execution role (if separate):
- Access to workgroup output S3 location
- Read access to the Iceberg warehouse data
