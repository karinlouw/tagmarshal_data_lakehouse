# TagMarshal Data Lakehouse

A local-first, AWS-ready data lakehouse for golf course telemetry data.

## Pipeline Overview

```
Source Files → Bronze (raw) → Silver (cleaned) → Gold (analytics) → Dashboard
```

---

## Pipeline Steps

### Step 0: Start Infrastructure

```bash
just up
```

Starts MinIO, Iceberg REST, Trino, Spark, Airflow, and PostgreSQL.

### Step 1: Source Data

Place CSV or JSON files in the `data/` folder.

**Supported formats:**
- CSV (flattened): `rounds.csv` with `locations[N].startTime` columns
- JSON (MongoDB): `rounds.json` with nested `locations` array

### Step 2: Bronze Layer (Raw Ingestion)

Upload raw files to the landing zone without transformation.

```bash
just bronze-upload course_id=bradshawfarmgc input=data/rounds.csv
```

**What happens:**
- File validated (required columns: `_id`, `course`, `locations`)
- Uploaded to MinIO: `s3://tm-lakehouse-landing/course_id=X/ingest_date=Y/`
- Idempotent: skips if file already exists

**Schema:** See `pipeline/bronze/schema.md`

### Step 3: Silver Layer (Transformation)

Transform raw data into clean, queryable Iceberg tables.

```bash
just silver course_id=bradshawfarmgc ingest_date=2025-06-28
```

**What happens:**
- Reads Bronze CSV/JSON from MinIO
- Explodes `locations[]` array → one row per GPS fix
- Deduplicates by `(round_id, fix_timestamp)`
- Derives fields: `nine_number`, `geometry_wkt`, `event_date`
- Writes to Iceberg table: `silver.fact_telemetry_event`

**Schema:** See `pipeline/silver/schema.md`

### Step 4: Gold Layer (Analytics)

Build aggregated views for analysis using dbt.

```bash
just gold
```

**What happens:**
- dbt reads Silver Iceberg table via Trino
- Builds analytical models:
  - `pace_summary_by_round` - Round performance metrics
  - `device_health_errors` - Device issue tracking
  - `signal_quality_rounds` - GPS quality analysis
  - `data_quality_overview` - Completeness scores
  - `critical_column_gaps` - Missing data analysis
  - `course_configuration_analysis` - Course setup patterns

**Schema:** See `pipeline/gold/schema.md`

### Step 5: View Dashboard

Explore data quality and insights via Streamlit.

```bash
just dashboard
```

Opens http://localhost:8501 with:
- Executive summary
- Data quality analysis
- Course analysis
- Course map (GIS)
- Critical gaps

---

## Backfill & Daily Operations

### Backfill Historical Data

Process all pending course/dates from the ingestion registry:

```bash
python pipeline/scripts/backfill.py
```

**Features:**
- Tracks processed files in PostgreSQL registry
- Resumable: continues from last successful record
- Retry logic for failed jobs
- Parallel batch processing

### Daily Updates (Automated)

Airflow DAGs run the pipeline automatically:

```
bronze_ingest_dag → silver_etl_dag → gold_dbt_dag
```

Trigger manually:
```bash
just airflow-trigger dag_id=silver_etl
```

---

## Project Structure

```
.
├── pipeline/                 # Core ETL pipeline (template-able)
│   ├── bronze/               # Bronze layer (raw ingestion)
│   ├── silver/               # Silver layer (transformation)
│   ├── gold/                 # Gold layer (dbt analytics)
│   ├── orchestration/        # Airflow DAGs
│   ├── infrastructure/       # Docker, database, services
│   ├── queries/              # SQL queries
│   ├── lib/                  # Shared Python utilities
│   ├── scripts/              # Backfill & utilities
│   └── docs/                 # Technical documentation
├── dashboard/                # Streamlit app (swappable)
├── data/                     # Sample data files (swappable)
├── config/                   # Environment configs
├── docs/                     # User guides & learning
├── notebooks/                # Jupyter exploration
├── docker-compose.yml
├── Justfile
└── README.md
```

---

## Quick Reference

| Task | Command |
|------|---------|
| Start stack | `just up` |
| Stop stack | `just down` |
| Check status | `just status` |
| Upload to Bronze | `just bronze-upload course_id=X input=file.csv` |
| Transform to Silver | `just silver course_id=X ingest_date=YYYY-MM-DD` |
| Build Gold models | `just gold` |
| View dashboard | `just dashboard` |
| Query with Trino | `just trino-query "SELECT * FROM silver.fact_telemetry_event LIMIT 10"` |
| Run all commands | `just --list` |

---

## Documentation

- **Pipeline walkthrough:** `docs/learning/pipeline_walkthrough.md`
- **Command reference:** `docs/learning/command_reference.md`
- **Technical architecture:** `pipeline/docs/architecture.md`
- **Layer schemas:** `pipeline/{bronze,silver,gold}/schema.md`

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Storage | MinIO (local) / S3 (AWS) |
| Table Format | Apache Iceberg |
| ETL | Apache Spark |
| SQL Engine | Trino |
| Orchestration | Apache Airflow |
| Transforms | dbt |
| Dashboard | Streamlit |
