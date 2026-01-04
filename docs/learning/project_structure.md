# Project Structure Guide

This document explains the folder structure of the TagMarshal Data Lakehouse project. Everything is organized to be simple and clear for junior developers.

## Root Directory

```
.
├── README.md              # Main project overview and quick start
├── Justfile              # Command shortcuts (use `just --list` to see all)
├── docker-compose.yml    # Docker services (MinIO, Spark, Trino, Airflow, etc.)
├── .gitignore            # Files to ignore in git (logs, cache, etc.)
│
├── config/               # Configuration files
├── data/                 # Sample data files (gitignored)
├── dashboard/            # Streamlit dashboard
├── docs/                 # All documentation
├── infrastructure/      # Infrastructure configuration
├── jobs/                 # Spark ETL jobs
├── monitoring/          # Monitoring & alerting configs
├── notebooks/           # Jupyter notebooks
├── orchestration/        # Airflow DAGs
├── queries/             # SQL exploration queries
├── schemas/             # Table schemas & DDLs
├── scripts/              # Utility scripts
├── tests/               # Integration & quality tests
├── transform/            # dbt project
└── validations/          # Data validation rules
```

## Key Directories Explained

### `config/`
Environment configuration files:
- `local.env` - Local development settings
- `aws.env` - AWS production settings

**When to edit:** When you need to change database connections, S3 buckets, or API keys.

### `dashboard/`
Streamlit web application for data quality visualization:
- `app.py` - Main dashboard code
- `requirements.txt` - Python dependencies

**Run it:** `just dashboard`

### `docs/`
All project documentation:
- `learning/` - Learning guides (start here!)
- `project/` - Architecture and design docs
- `proposals/` - Client proposals

**Start here:** `docs/learning/pipeline_walkthrough.md`

### `infrastructure/`
Infrastructure configuration files:
- `database/` - Database migration scripts (SQL)
- `services/` - Docker service configurations (Trino, etc.)

**When to edit:** When you need to add database tables or modify service configs.

### `jobs/spark/`
PySpark ETL jobs that transform data:
- `silver_etl.py` - Transforms Bronze → Silver
- `lib/` - Shared utility code

**How it works:** Airflow runs these jobs via `spark-submit`

### `orchestration/airflow/`
Airflow configuration and DAGs:
- `dags/` - DAG definitions (Bronze, Silver, Gold)
- `requirements.txt` - Python dependencies for Airflow

**Access UI:** http://localhost:8080 (after `just up`)

### `scripts/`
Utility scripts for manual operations:
- `backfill.py` - Bulk data processing with resume capability

**When to use:** For one-off tasks or bulk operations

### `transform/dbt_project/`
dbt project for building Gold layer:
- `models/gold/` - SQL models for analytics
- `dbt_project.yml` - dbt configuration

**Run it:** `just gold`

## Data Flow

```
1. Upload CSV/JSON → data/ (or Bronze bucket)
2. Bronze DAG → Ingests to Bronze layer
3. Silver ETL → Transforms to Silver layer
4. Gold dbt → Builds analytics models
5. Dashboard → Visualizes results
```

## Where to Start

1. **New to the project?** → Read `README.md` and `docs/learning/pipeline_walkthrough.md`
2. **Want to run something?** → Check `docs/learning/command_reference.md`
3. **Understanding architecture?** → Read `docs/project/PROJECT_OVERVIEW.md`
4. **Working on code?** → Each folder has its own `README.md`

## File Naming Conventions

- **Python files**: `snake_case.py`
- **SQL files**: `snake_case.sql`
- **Config files**: `kebab-case.env`
- **Documentation**: `kebab-case.md`

## Important Files

- **`Justfile`** - Your best friend! Use `just <command>` instead of long Docker commands
- **`docker-compose.yml`** - Defines all services (don't edit unless you know what you're doing)
- **`config/local.env`** - Local settings (safe to edit)

## Questions?

- Check the `README.md` in each folder
- Read the learning guides in `docs/learning/`
- Look at code comments (they're written to be helpful!)

