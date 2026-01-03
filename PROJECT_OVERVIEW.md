# Tagmarshal Data Lakehouse

A **local-first, AWS-ready** data lakehouse for golf course **round data**. Built with modern data engineering best practices: medallion architecture, Apache Iceberg tables, and full observability.

> **Note:** This lakehouse processes **round strings** (pre-processed data from TagMarshal's system), not raw GPS telemetry. The raw GPS pings have already been processed into structured rounds with pace calculations, hole assignments, and section tracking.

---

## 🎯 What This Project Does

Consolidates processed round data from TagMarshal into a scalable analytics platform:

```
CSV/JSON Files (or API) → Bronze (raw) → Silver (cleaned) → Gold (analytics)
```

**Key capabilities:**
- Ingest CSV files with nested location arrays (18-117 sections per round, avg ~40)
- Ingest JSON files from TagMarshal API (MongoDB export format)
- Transform wide CSVs or nested JSON into queryable long-format Iceberg tables
- Generate analytics-ready summaries (pace of play, round metrics, data quality)
- Full data lineage, quarantine for bad data, and run observability
- **API-ready**: Switch from local files to API with a single config change

**Current scope (Phase 1):** 5 pilot courses with historical CSV exports, validating the data model and transformation logic locally before AWS deployment.

---

## 🏗️ Architecture

### Tech Stack

| Layer | Local (Docker) | AWS Production |
|-------|----------------|----------------|
| **Storage** | MinIO | S3 |
| **Table Format** | Apache Iceberg | Apache Iceberg |
| **Catalog** | Iceberg REST | Glue Data Catalog |
| **ETL Engine** | Apache Spark 3.5 | AWS Glue (Spark) |
| **SQL Engine** | Trino | Amazon Athena |
| **Orchestration** | Apache Airflow | MWAA / Self-managed Airflow |
| **Transforms** | dbt Core (Trino) | dbt Core (Athena) |

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA LAYERS                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  📥 BRONZE (Landing Zone)                                           │
│     • Raw CSV or JSON files exactly as received                     │
│     • Supports: local files or TagMarshal API                       │
│     • Partitioned: course_id/ingest_date                            │
│     • Bucket: tm-lakehouse-landing-zone                             │
│                                                                     │
│  ⚙️ SILVER (Source Store)                                           │
│     • Cleaned, validated, long-format Iceberg table                 │
│     • Table: silver.fact_telemetry_event                            │
│     • Partitioned: course_id, event_date                            │
│     • Bucket: tm-lakehouse-source-store                             │
│                                                                     │
│  🏆 GOLD (Serve Layer)                                              │
│     • Pre-aggregated analytics tables (dbt models)                  │
│     • Tables: pace_summary_by_round, signal_quality_rounds, etc.    │
│     • Bucket: tm-lakehouse-serve                                    │
│                                                                     │
│  🚫 QUARANTINE                                                      │
│     • Invalid rows (bad coordinates, parse failures)                │
│     • Bucket: tm-lakehouse-quarantine                               │
│                                                                     │
│  📋 OBSERVABILITY                                                   │
│     • Run summaries, dbt artifacts, audit logs                      │
│     • Bucket: tm-lakehouse-observability                            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (running)
- **just** command runner (`brew install just` on macOS)

### 1. Start the Stack

```bash
just up          # Start all services
just ui          # Show service URLs
```

**Service URLs:**
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Trino | http://localhost:8081 | — |
| Spark UI | http://localhost:8082 | — |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |

### 2. Run the Full Pipeline

```bash
# Option A: Run everything at once
just pipeline-all 2025-01-03

# Option B: Run each stage manually
just bronze-upload-all 2025-01-03   # Upload CSVs to Bronze
just silver-all 2025-01-03          # Transform to Silver Iceberg
just gold                           # Build Gold dbt models
```

### 3. Query the Data

```bash
docker exec -it trino trino
```

```sql
-- Check Silver table
SELECT course_id, COUNT(*) as fixes 
FROM iceberg.silver.fact_telemetry_event 
GROUP BY course_id;

-- Check Gold analytics
SELECT * FROM iceberg.gold.pace_summary_by_round LIMIT 10;
```

---

## 📁 Project Structure

```
tagmarshal_data_lakehouse/
├── config/
│   ├── local.env              # Local Docker config (MinIO, etc.)
│   └── aws.env                # AWS config template
├── data/                      # Input CSV/JSON files (mounted to containers)
│   ├── americanfalls.rounds.csv
│   ├── erinhills.rounds.csv
│   ├── indiancreek.rounds_json.json  # Sample JSON (API format)
│   └── ...
├── db/migrations/             # PostgreSQL migrations (ingestion registry)
├── docker/trino/etc/          # Trino catalog configuration
├── docs/
│   ├── runbook_local_dev.md   # Detailed junior-friendly guide
│   ├── aws_cutover.md         # AWS deployment guide
│   └── learning/              # Concept explanations (Iceberg, dbt, etc.)
├── jobs/spark/
│   ├── silver_etl.py          # Spark ETL: Bronze CSV/JSON → Silver Iceberg
│   └── lib/tm_lakehouse/      # Shared Python utilities
│       ├── api.py             # API client & JSON normalization
│       ├── bronze.py          # Bronze upload (CSV & JSON support)
│       ├── config.py          # Environment configuration
│       └── ...
├── orchestration/airflow/
│   ├── dags/
│   │   ├── bronze_ingest_dag.py
│   │   ├── silver_etl_dag.py
│   │   └── gold_dbt_dag.py
│   └── logs/
├── transform/dbt_project/
│   ├── models/
│   │   ├── sources.yml        # Silver source definition
│   │   └── gold/              # Gold model SQL files
│   ├── tests/                 # Custom dbt tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker-compose.yml         # Full local stack definition
├── Justfile                   # Developer command recipes
└── README.md
```

---

## ⚡ Key Commands

### Stack Management

| Command | Description |
|---------|-------------|
| `just up` | Start all Docker services |
| `just down` | Stop all services |
| `just restart` | Restart containers (no rebuild) |
| `just rebuild` | Rebuild and restart containers |
| `just nuke` | ☢️ Delete everything and start fresh |
| `just health` | Show container status |
| `just logs` | Follow all service logs |

### Pipeline Execution

> **Note:** `[date]` = ingest date in `YYYY-MM-DD` format. Defaults to today if omitted.

| Command | Description |
|---------|-------------|
| `just bronze-upload <course> <path> [date]` | Upload single CSV/JSON file |
| `just bronze-upload-all [date]` | Upload all files in data/ |
| `just silver <course> <date> <prefix>` | Process single course |
| `just silver-all [date]` | Process all courses |
| `just gold` | Run dbt Gold models |
| `just pipeline-all [date]` | Full Bronze → Silver → Gold |
| `just status` | Show recent DAG run status |

**Examples:**
```bash
just bronze-upload americanfalls data/americanfalls.rounds.csv 2025-01-03
just bronze-upload-all                    # Uses today's date
just silver-all 2025-01-03
```

### Data Inspection

| Command | Description |
|---------|-------------|
| `just tree [bucket]` | Show MinIO bucket structure |
| `just bucket-sizes` | Show size of each bucket |
| `just ingestion-status [date]` | Show what was ingested |
| `just ingestion-history <course>` | Full history for a course |

---

## 🔄 Pipeline Flow

### Bronze Ingest (Airflow DAG)

```
data/*.csv or *.json → Airflow → MinIO (Landing Zone)
       ↑                         └── course_id=X/ingest_date=Y/file.csv|json
  (or API fetch)
```

- Supports both CSV and JSON file formats
- Auto-detects format from file extension/content
- JSON files can be MongoDB export format (nested `$oid`, `$date`)
- API-ready: when you get API access, switch `TM_DATA_SOURCE=api`
- Idempotent: skips existing files

### Silver ETL (Spark Job)

```
Landing Zone (CSV or JSON) → Spark → Iceberg Table
                                     └── silver.fact_telemetry_event
```

Key transformations:
1. **Auto-detect** input format (CSV with flattened columns or JSON with nested arrays)
2. **Explode** location arrays into long format (one row per GPS fix)
3. **Normalize** MongoDB types (`$oid` → string, `$date` → timestamp)
4. **Parse** coordinates from `locations[i].fixCoordinates[0/1]`
5. **Validate** lat/lon ranges, quarantine invalid rows
6. **Deduplicate** by `(round_id, fix_timestamp)`, preferring `is_cache=true`
7. **Derive** `event_date` for partition pruning

### Gold Models (dbt)

```
Silver Iceberg → dbt → Gold Iceberg Tables
```

| Model | Purpose |
|-------|---------|
| `pace_summary_by_round` | Avg pace, pace gap, positional gap per round |
| `signal_quality_rounds` | Signal quality metrics per round |
| `device_health_errors` | Device battery and error tracking |

---

## ⚙️ Configuration

### Environment Switching

The stack switches between local and AWS by swapping env files:

```bash
# Local development
docker compose --env-file config/local.env up -d

# AWS (not via Docker Compose; uses IAM roles)
# See docs/aws_cutover.md
```

### Key Environment Variables

| Variable | Local Default | Purpose |
|----------|---------------|---------|
| `TM_S3_ENDPOINT` | `http://minio:9000` | S3 endpoint (blank for AWS) |
| `TM_BUCKET_LANDING` | `tm-lakehouse-landing-zone` | Bronze bucket |
| `TM_BUCKET_SOURCE` | `tm-lakehouse-source-store` | Silver bucket |
| `TM_BUCKET_SERVE` | `tm-lakehouse-serve` | Gold bucket |
| `TM_ICEBERG_WAREHOUSE_SILVER` | `s3://tm-lakehouse-source-store/warehouse` | Iceberg warehouse |
| `TM_DATA_SOURCE` | `file` | Data source: `file` or `api` |
| `TM_API_BASE_URL` | `https://api.tagmarshal.com` | TagMarshal API endpoint |
| `TM_API_KEY` | (empty) | API key (set when you get access) |

---

## 📊 Silver Table Schema

**Table:** `iceberg.silver.fact_telemetry_event`

Each row represents one **location record** (processed section) from a round.

| Column | Type | Description |
|--------|------|-------------|
| `round_id` | STRING | Unique round identifier |
| `course_id` | STRING | Course identifier (partition) |
| `ingest_date` | STRING | When data was ingested |
| `fix_timestamp` | TIMESTAMP | When location was recorded |
| `event_date` | DATE | Date of record (partition) |
| `start_hole` | INT | Which hole round started on (shotgun support) |
| `is_nine_hole` | BOOLEAN | Is this a 9-hole round? |
| `current_nine` | INT | Which nine (1=front, 2=back) |
| `hole_number` | INT | Current hole (1-18) |
| `section_number` | INT | Overall section (1-53 for 18 holes) |
| `hole_section` | INT | Section within hole (1=tee, 2=fairway, 3=green) |
| `nine_number` | INT | Derived: 1 if hole ≤9, else 2 |
| `longitude` | DOUBLE | GPS longitude |
| `latitude` | DOUBLE | GPS latitude |
| `pace` | DOUBLE | Current pace vs goal (seconds) |
| `pace_gap` | DOUBLE | Gap to group ahead (seconds) |
| `battery_percentage` | DOUBLE | Device battery |
| `geometry_wkt` | STRING | WKT format point |

**Multi-course configurations supported:**
- Shotgun starts (rounds starting on hole 10)
- 9-hole vs 18-hole rounds
- 9-hole courses played twice (distinguished by `current_nine`)

---

## 🔍 Observability

### Run Tracking

Each pipeline stage writes run summaries:

```
tm-lakehouse-observability/
├── bronze/run_id=<timestamp>.json
└── silver/course_id=X/ingest_date=Y/run_id=<timestamp>.json
```

### Ingestion Registry

A PostgreSQL table tracks all ingestions:

```bash
just registry-init        # Create table (run once)
just ingestion-status     # Show today's ingestions
just ingestion-missing    # Find courses not ingested today
```

### Quarantine

Invalid rows are preserved for debugging:

```
tm-lakehouse-quarantine/
└── silver/course_id=X/ingest_date=Y/run_id=<timestamp>/
    └── *.json (invalid rows with original values)
```

---

## 🔧 Troubleshooting

### DAG stuck in "queued"

```bash
docker logs airflow | grep ERROR
just restart-service airflow
```

### Spark job fails

```bash
docker logs spark          # Check Spark logs
docker exec spark ls /opt/tagmarshal/jobs/spark/  # Verify job files
```

### MinIO buckets not created

```bash
just rebuild               # Recreate containers
docker logs minio-init     # Check bucket creation
```

### "Table not found" in Trino

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;
```

If empty, Silver ETL hasn't run yet. Run `just silver-all`.

---

## 📚 Additional Documentation

| Document | Purpose |
|----------|---------|
| `docs/runbook_local_dev.md` | Detailed step-by-step guide for juniors |
| `docs/aws_cutover.md` | AWS deployment instructions |
| `docs/learning/iceberg_basics.md` | What is Apache Iceberg? |
| `docs/learning/lakehouse_layers.md` | Medallion architecture explained |
| `docs/learning/dbt_testing_101.md` | dbt testing concepts |
| `docs/learning/airflow_101.md` | Airflow DAG basics |
| `docs/learning/data_model_and_pipeline.md` | **Data model & how to run** |

---

## 🎓 Design Philosophy

This project is intentionally **junior-friendly**:

1. **Clear folder structure** — each component has its place
2. **Verbose Just commands** — pretty output shows what's happening
3. **Fail fast** — raise errors instead of silently skipping
4. **Full observability** — every run is logged and traceable
5. **Config-only switching** — same code works local → AWS
6. **Learning docs** — explains industry jargon in context

---

## 📄 License

Internal project — Tagmarshal.

