# TagMarshal Data Lakehouse

A **local-first, AWS-ready** data lakehouse for golf course round data. Built with modern data engineering best practices: medallion architecture, Apache Iceberg tables, and full observability.

> **Note:** This lakehouse processes **round strings** (pre-processed data from TagMarshal's system), not raw GPS telemetry. The raw GPS pings have already been processed into structured rounds with pace calculations, hole assignments, and section tracking.

## 🚀 Quick Start

### 1. Start the stack
```bash
just up
```

### 2. Ingest data (Bronze layer)
```bash
just bronze-upload course_id=<course_id> input=<path_to_file>
```

### 3. Transform to Silver
```bash
just silver course_id=<course_id> ingest_date=YYYY-MM-DD
```

### 4. Build analytics (Gold layer)
```bash
just gold
```

### 5. View dashboard
```bash
just dashboard
```

## 📁 Project Structure

```
.
├── config/              # Environment configs (local.env, aws.env)
├── data/                # Sample CSV/JSON files (gitignored)
├── dashboard/           # Streamlit data quality dashboard
├── docs/                # Documentation
│   ├── learning/        # Learning guides (pipeline, dbt, airflow, etc.)
│   ├── project/         # Project overview and architecture
│   └── proposals/       # Client proposals
├── infrastructure/      # Infrastructure configuration
│   ├── database/        # Database migrations
│   └── services/        # Docker service configs (Trino, etc.)
├── jobs/                # Spark ETL jobs
│   └── spark/           # Silver ETL transformation
├── monitoring/          # Monitoring & alerting configs
├── notebooks/           # Jupyter notebooks for exploration
├── orchestration/       # Airflow DAGs and config
│   └── airflow/         # DAG definitions
├── queries/             # SQL exploration queries
│   ├── exploration/     # Dashboard and analysis queries
│   └── examples/        # Example queries
├── schemas/             # Table schemas and DDLs
│   ├── bronze/          # Bronze layer schemas
│   ├── silver/          # Silver layer schemas
│   └── gold/            # Gold layer schemas
├── scripts/             # Utility scripts (backfill, etc.)
├── tests/               # Integration and data quality tests
│   ├── integration/     # End-to-end pipeline tests
│   ├── data_quality/    # Data quality validation tests
│   └── fixtures/        # Test data files
├── transform/           # dbt project for Gold layer
│   └── dbt_project/     # dbt models and config
└── validations/         # Data validation rules
    ├── rules/           # Validation rule definitions
    └── thresholds/      # Quality thresholds
```

## 🏗️ Architecture

**Tech Stack:**
- **Storage**: MinIO (local) / S3 (AWS)
- **Table Format**: Apache Iceberg
- **ETL**: Apache Spark
- **SQL Engine**: Trino (local) / Athena (AWS)
- **Orchestration**: Apache Airflow
- **Transforms**: dbt

**Data Flow:**
```
CSV/JSON → Bronze (raw) → Silver (cleaned) → Gold (analytics)
```

## 📚 Documentation

- **Getting Started**: `docs/runbook_local_dev.md`
- **Pipeline Walkthrough**: `docs/learning/pipeline_walkthrough.md`
- **Command Reference**: `docs/learning/command_reference.md`
- **Project Overview**: `docs/project/PROJECT_OVERVIEW.md`
- **AWS Migration**: `docs/aws_cutover.md`

## 🔧 Configuration

### Local Development
Edit `config/local.env` to change local settings.

### Switch to API Source
When you get API access, update `config/local.env`:
```bash
TM_DATA_SOURCE=api
TM_API_KEY=your-api-key-here
```

## 💡 For Junior Developers

This project is designed to be **simple and clear**:
- Each folder has a `README.md` explaining its purpose
- Code is well-commented and straightforward
- Learning guides in `docs/learning/` explain each component
- Use `just` commands (see `docs/learning/command_reference.md`) instead of complex Docker commands

## 🛠️ Common Tasks

```bash
# View all available commands
just --list

# Check service status
just status

# Run data quality checks
just dq

# Query data with Trino
just trino-query "SELECT * FROM iceberg.silver.fact_telemetry_event LIMIT 10"
```

## 📊 Dashboard

The Streamlit dashboard shows data quality metrics and insights:
```bash
just dashboard
```

Access at http://localhost:8501
