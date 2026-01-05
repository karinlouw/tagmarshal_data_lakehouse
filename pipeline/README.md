# Pipeline

Core ETL pipeline for TagMarshal data lakehouse.

## Structure

```
pipeline/
├── bronze/          # Raw data ingestion
│   ├── ingest.py    # Upload utilities
│   └── schema.md    # Bronze schema docs
├── silver/          # Data transformation
│   ├── etl.py       # Spark ETL script
│   └── schema.md    # Silver schema docs
├── gold/            # Analytics (dbt)
│   ├── models/      # dbt SQL models
│   └── schema.md    # Gold schema docs
├── orchestration/   # Airflow DAGs
│   └── dags/        # DAG definitions
├── infrastructure/  # Docker & configs
│   ├── docker/      # Dockerfiles
│   ├── database/    # SQL migrations
│   └── services/    # Trino config
├── lib/             # Shared utilities
│   ├── config.py    # Environment config
│   └── s3.py        # S3 client
├── queries/         # SQL queries
│   ├── exploration/ # Dashboard queries
│   └── examples/    # Sample queries
├── scripts/         # Utilities
│   └── backfill.py  # Backfill script
├── tests/           # Integration tests
├── monitoring/      # Alerting configs
└── docs/            # Technical docs
```

## Layer Responsibilities

| Layer | Purpose | Technology |
|-------|---------|------------|
| Bronze | Store raw data exactly as received | MinIO/S3 |
| Silver | Clean, transform, conform | Spark + Iceberg |
| Gold | Aggregate for analytics | dbt + Trino |

## Quick Start

```bash
# 1. Upload raw data
just bronze-upload course_id=test input=data/rounds.csv

# 2. Transform to Silver
just silver course_id=test ingest_date=2025-01-01

# 3. Build Gold models
just gold
```

## Documentation

- `docs/architecture.md` - System architecture
- `docs/data_model_and_pipeline.md` - Data model details
- `docs/how_to_debug_runs.md` - Troubleshooting guide
- `{bronze,silver,gold}/schema.md` - Layer schemas

