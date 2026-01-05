# Project Structure

This document explains the folder structure of the TagMarshal Data Lakehouse.

## Top-Level Structure

```
.
├── pipeline/          # Core ETL pipeline (template-able)
├── dashboard/         # Streamlit data quality app (swappable)
├── data/              # Sample data files (swappable)
├── config/            # Environment configs
├── docs/              # User guides and learning
├── notebooks/         # Jupyter exploration
├── docker-compose.yml # Docker services
├── Justfile           # Command runner
└── README.md          # Project overview
```

## Pipeline Folder

The `pipeline/` folder contains all ETL code and can be used as a template:

```
pipeline/
├── bronze/            # Raw data ingestion
│   ├── ingest.py      # Upload utilities
│   └── schema.md      # Bronze schema docs
├── silver/            # Data transformation
│   ├── etl.py         # Spark ETL script
│   ├── schema.md      # Silver schema
│   └── data_dictionary.md
├── gold/              # Analytics (dbt)
│   ├── models/        # dbt SQL models
│   ├── tests/         # dbt tests
│   └── schema.md      # Gold schema docs
├── orchestration/     # Airflow DAGs
│   └── dags/          # DAG definitions
├── infrastructure/    # Docker & database
│   ├── docker/        # Dockerfiles
│   ├── database/      # SQL migrations
│   └── services/      # Trino config
├── lib/               # Shared Python code
│   └── tm_lakehouse/  # Package modules
├── queries/           # SQL queries
│   ├── exploration/   # Dashboard queries
│   └── examples/      # Sample queries
├── scripts/           # Utilities
│   └── backfill.py
├── tests/             # Integration tests
├── monitoring/        # Alerting configs
└── docs/              # Technical docs
```

## Swappable Components

For a new project, you would keep `pipeline/` as-is and swap:

1. **`data/`** - Replace with your data files
2. **`dashboard/`** - Build a new Streamlit app
3. **`config/`** - Update environment variables

## Key Files

| File | Purpose |
|------|---------|
| `Justfile` | All pipeline commands |
| `docker-compose.yml` | Docker service definitions |
| `config/local.env` | Local environment settings |
| `pipeline/silver/etl.py` | Main transformation script |
| `pipeline/gold/dbt_project.yml` | dbt configuration |
