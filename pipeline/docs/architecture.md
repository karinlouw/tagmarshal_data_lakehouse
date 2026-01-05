# Pipeline Architecture

## Overview

The TagMarshal data lakehouse follows a medallion architecture with three layers:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  CSV/JSON  ──►  BRONZE  ──►  SILVER  ──►  GOLD  ──►  DASHBOARD     │
│  (Source)      (Raw)        (Clean)      (Agg)      (Visualize)    │
│                                                                      │
│  ┌─────┐      ┌─────┐      ┌─────┐      ┌─────┐      ┌─────┐       │
│  │File │  ──► │MinIO│  ──► │Spark│  ──► │ dbt │  ──► │Strm │       │
│  └─────┘      └─────┘      └─────┘      └─────┘      └─────┘       │
│                  │            │            │                        │
│                  ▼            ▼            ▼                        │
│              ┌──────────────────────────────────┐                  │
│              │         Apache Iceberg           │                  │
│              │      (Table Format Layer)        │                  │
│              └──────────────────────────────────┘                  │
│                              │                                      │
│                              ▼                                      │
│              ┌──────────────────────────────────┐                  │
│              │            Trino                 │                  │
│              │       (Query Engine)             │                  │
│              └──────────────────────────────────┘                  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

### Storage: MinIO / S3

Object storage for all data layers:
- `tm-lakehouse-landing` - Bronze (raw files)
- `tm-lakehouse-source` - Silver (Iceberg tables)
- `tm-lakehouse-serve` - Gold (Iceberg tables)
- `tm-lakehouse-quarantine` - Invalid data
- `tm-lakehouse-observability` - Logs and metrics

### Table Format: Apache Iceberg

Provides ACID transactions, schema evolution, and time travel:
- Managed by Iceberg REST catalog (local) or AWS Glue (production)
- Supports both Spark and Trino access
- Partitioned by `course_id` and `ingest_date`

### ETL Engine: Apache Spark

Transforms Bronze to Silver:
- Runs inside Docker container
- Reads CSV/JSON, writes Iceberg
- Handles array explosion, deduplication, type conversion

### SQL Engine: Trino

Interactive queries on Iceberg tables:
- Used by dbt for Gold layer builds
- Powers the Streamlit dashboard
- Supports standard SQL

### Transforms: dbt

Builds Gold layer analytics:
- SQL-based transformations
- Version controlled models
- Data quality tests

### Orchestration: Apache Airflow

Schedules and monitors pipeline:
- DAGs for Bronze, Silver, Gold
- Backfill support
- Alerting on failures

## Local vs AWS

| Component | Local | AWS |
|-----------|-------|-----|
| Storage | MinIO | S3 |
| Catalog | Iceberg REST | Glue |
| Query | Trino | Athena |
| Config | `config/local.env` | `config/aws.env` |

## File Paths

```
pipeline/
├── bronze/          # Ingestion code
├── silver/          # Spark ETL
├── gold/            # dbt models
├── orchestration/   # Airflow DAGs
├── infrastructure/  # Docker, database
├── lib/             # Shared Python code
├── queries/         # SQL queries
└── scripts/         # Backfill utilities
```

## Key Processes

### Ingestion (Bronze)

1. Validate input file (CSV or JSON)
2. Generate S3 key: `course_id=X/ingest_date=Y/filename`
3. Upload to MinIO landing bucket
4. Log to ingestion registry

### Transformation (Silver)

1. Read Bronze files from S3
2. Detect format (CSV vs JSON)
3. Parse and explode locations array
4. Apply data quality rules
5. Deduplicate records
6. Write to Iceberg table

### Analytics (Gold)

1. dbt reads Silver via Trino
2. Applies SQL transformations
3. Runs data quality tests
4. Writes aggregated tables

### Dashboard

1. Connects to Trino
2. Runs SQL queries
3. Renders charts and tables
4. Shows data quality metrics

