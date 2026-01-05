# Local Development Runbook

> **For junior data engineers:** This guide explains not just *how* to run the pipeline, but *why* each step exists. Industry jargon is explained in context.

---

## Table of Contents
1. [What is a Data Lakehouse?](#what-is-a-data-lakehouse)
2. [The Medallion Architecture (Bronze → Silver → Gold)](#the-medallion-architecture-bronze--silver--gold)
3. [Local Stack Overview](#local-stack-docker-compose)
4. [Quick Start](#quick-start)
5. [Understanding Each Pipeline Stage](#understanding-each-pipeline-stage)
6. [Available Commands](#available-commands)
7. [Key Features](#key-features)
8. [Troubleshooting](#troubleshooting)
9. [Entity Relationship Diagram (ERD)](#entity-relationship-diagram-erd)
10. [Glossary](#glossary)

---

## What is a Data Lakehouse?

A **Data Lakehouse** combines the best of two worlds:
- **Data Lake**: Cheap, scalable storage for raw files (like S3)
- **Data Warehouse**: Structured, queryable tables with SQL

**Why use one?** Traditional data warehouses are expensive and rigid. Data lakes are cheap but messy. A lakehouse gives you cheap storage with warehouse-like query performance using formats like **Apache Iceberg**.

### Key Components in Our Stack

| Component | What it does | Analogy |
|-----------|--------------|---------|
| **MinIO** | S3-compatible object storage | Your hard drive, but for the cloud |
| **Iceberg** | Table format that makes files queryable | A smart filing system for your data lake |
| **Spark** | Distributed data processing engine | A factory that transforms raw materials |
| **Trino** | SQL query engine | Your window to ask questions about data |
| **Airflow** | Workflow orchestration | A conductor that runs tasks in order |
| **dbt** | Data transformation tool | A template engine for SQL |

---

## The Medallion Architecture (Bronze → Silver → Gold)

The **Medallion Architecture** is a design pattern that organizes data into three layers. Think of it like refining raw materials:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        THE MEDALLION ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   📥 RAW DATA                                                               │
│      (CSV files)                                                            │
│           │                                                                 │
│           ▼                                                                 │
│   ┌───────────────┐                                                         │
│   │    BRONZE     │  ← "Landing Zone" - Raw data exactly as received        │
│   │  (Raw Layer)  │    • No transformations                                 │
│   │               │    • Preserves original format                          │
│   │  Format: CSV  │    • Enables replay/debugging                           │
│   └───────┬───────┘                                                         │
│           │                                                                 │
│           │  Spark ETL (Extract, Transform, Load)                           │
│           ▼                                                                 │
│   ┌───────────────┐                                                         │
│   │    SILVER     │  ← "Cleaned Layer" - Validated, structured data         │
│   │(Conformed)    │    • Schema enforced                                    │
│   │               │    • Data types correct                                 │
│   │Format: Iceberg│    • Invalid rows quarantined                           │
│   └───────┬───────┘                                                         │
│           │                                                                 │
│           │  dbt (SQL transformations)                                      │
│           ▼                                                                 │
│   ┌───────────────┐                                                         │
│   │     GOLD      │  ← "Analytics Layer" - Business-ready aggregations      │
│   │  (Analytics)  │    • Pre-computed summaries                             │
│   │               │    • Ready for dashboards                               │
│   │Format: Iceberg│    • Answers business questions                         │
│   └───────────────┘                                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why Three Layers?

| Layer | Purpose | Who Uses It | Example |
|-------|---------|-------------|---------|
| **Bronze** | Audit trail & replay | Data engineers (debugging) | Raw CSV exactly as received |
| **Silver** | Clean, validated data | Data analysts, ML engineers | `fix_latitude`, `fix_longitude` as proper floats |
| **Gold** | Business metrics | Business users, dashboards | "Average pace per round by course" |

### What Happens Between Each Layer?

#### Bronze → Silver (Spark ETL)
- **Extract**: Read raw CSV from Bronze storage
- **Transform**: 
  - Parse nested JSON fields (like `fixCoordinates`)
  - Convert strings to proper data types
  - Validate coordinates are within valid ranges
  - Split invalid rows to a "quarantine" bucket
- **Load**: Write clean data to Iceberg table

#### Silver → Gold (dbt)
- **Aggregate**: Summarize data (averages, counts, totals)
- **Enrich**: Join with reference data
- **Document**: Add descriptions and tests
- **Quality Gate**: Run automated tests before publishing

---

## Local Stack (Docker Compose)

When you run `just up`, these services start:

| Service | Port | URL | Purpose |
|---------|------|-----|---------|
| **Airflow** | 8080 | http://localhost:8080 | Orchestration UI (run & monitor pipelines) |
| **Trino** | 8081 | http://localhost:8081 | Query engine UI |
| **Spark** | 8082 | http://localhost:8082 | Spark Web UI (monitor jobs) |
| **MinIO** | 9001 | http://localhost:9001 | S3-compatible storage (view your files) |

> **Note:** Iceberg REST (port 8181) is an API-only service with no web UI. It's used internally by Spark and Trino.

### MinIO Credentials
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`

### Airflow Credentials
- **Username**: `admin`
- **Password**: `admin`

---

## Quick Start

### 1. Start All Services

```bash
just up
```

**What this does:**
- Starts Docker containers for MinIO, Spark, Trino, Airflow, etc.
- Creates S3 buckets automatically (`tm-lakehouse-landing-zone`, `tm-lakehouse-source-store`, `tm-lakehouse-serve`, etc.)
- Waits for services to be healthy

### 2. View Service URLs

```bash
just ui
```

**What this does:**
- Prints URLs for all service web interfaces
- Helpful to remember which port is which

### 3. Upload All CSVs to Bronze

```bash
just bronze-upload-all 2025-12-16
```

**What this does:**
- Scans the `data/` folder for all `.csv` files
- Extracts `course_id` from filename (e.g., `americanfalls.rounds.csv` → `americanfalls`)
- Uploads each file to Bronze layer in MinIO
- **Idempotent**: Skips files that already exist (safe to re-run)

### 4. Process All Courses Through Silver

```bash
just silver-all 2025-12-16
```

**What this does:**
- For each course, triggers the Silver ETL DAG in Airflow
- Spark reads Bronze CSV, transforms it, writes to Iceberg
- dbt runs quality tests (schema validation, freshness checks)

### 5. Build Gold Analytics Models

```bash
just gold
```

**What this does:**
- Runs dbt to build Gold layer tables
- Creates pre-aggregated analytics tables
- Ready for dashboards and reporting

---

## Understanding Each Pipeline Stage

### Stage 1: Bronze Upload (Raw Ingestion)

```
┌──────────────────────────────────────────────────────────────┐
│  BRONZE: What happens when you run `just bronze-upload-all`  │
└──────────────────────────────────────────────────────────────┘

  data/americanfalls.rounds.csv
           │
           ▼
  ┌─────────────────┐
  │  Validate CSV   │ ← Check: Is it a valid CSV? Any obvious errors?
  │  (basic checks) │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │ Check Idempotency│ ← Does this file already exist in S3?
  │  (skip if exists)│   If yes, skip. If no, upload.
  └────────┬────────┘
           │
           ▼
  s3://tm-lakehouse-landing-zone/
    └── course_id=americanfalls/
        └── ingest_date=2025-12-16/
            └── americanfalls.rounds.csv
```

**Key concept: Idempotency**
- Running the same command twice produces the same result
- No duplicate uploads, no errors on re-run
- Safe to retry after failures

### Stage 2: Silver ETL (Spark Transformation)

```
┌──────────────────────────────────────────────────────────────┐
│  SILVER: What happens when you run `just silver-all`         │
└──────────────────────────────────────────────────────────────┘

  s3://tm-lakehouse-landing-zone/course_id=americanfalls/...
           │
           ▼
  ┌─────────────────┐
  │  Spark reads    │ ← Load raw CSV into Spark DataFrame
  │  Bronze CSV     │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────────────────────────────────────────────┐
  │                   TRANSFORMATIONS                       │
  │                                                         │
  │  1. Parse nested JSON (fixCoordinates → lat, lon)       │
  │  2. Convert types (string → float, timestamp)           │
  │  3. Add metadata (ingest_date, course_id)               │
  │  4. Explode arrays (one row per location fix)           │
  └────────┬────────────────────────────────────────────────┘
           │
           ├──────────────────────┐
           │                      │
           ▼                      ▼
  ┌─────────────────┐    ┌─────────────────┐
  │  Valid rows →   │    │ Invalid rows →  │
  │  Silver Iceberg │    │   Quarantine    │
  │                 │    │                 │
  │ (lat between    │    │ (lat outside    │
  │  -90 and 90)    │    │  valid range)   │
  └─────────────────┘    └─────────────────┘
```

**Key concepts:**
- **ELT**: Extract (read from source), **Load** (save raw to Bronze), then **Transform** (clean in Silver).
- **Quarantine**: Bad data goes to a separate bucket for investigation
- **Iceberg**: Enables time-travel, schema evolution, and efficient queries
- **Append Mode**: For large datasets (650 courses × 7 years), we use APPEND with delete-then-insert for idempotency

**Silver Table Schema (iceberg.silver.fact_telemetry_event):**

| Column            | Type      | Description                           |
|-------------------|-----------|---------------------------------------|
| round_id          | STRING    | Unique round identifier (_id)         |
| course_id         | STRING    | Course identifier (partition column)  |
| ingest_date       | STRING    | When data was ingested (YYYY-MM-DD)   |
| fix_timestamp     | TIMESTAMP | GPS fix timestamp                     |
| event_date        | DATE      | Date of the fix (partition column)    |
| location_index    | INT       | Index within locations[] array        |
| hole_number       | INT       | Current hole number                   |
| longitude         | DOUBLE    | GPS longitude (fixCoordinates[0])     |
| latitude          | DOUBLE    | GPS latitude (fixCoordinates[1])      |
| pace              | DOUBLE    | Current pace metric                   |
| battery_percentage| DOUBLE    | Device battery level                  |
| geometry_wkt      | STRING    | WKT format: POINT(lon lat)           |

**Note:** GeoJSON uses `[longitude, latitude]` order! `fixCoordinates[0]` = longitude, `fixCoordinates[1]` = latitude.

### Stage 3: Silver Quality Gate (dbt Tests)

```
┌──────────────────────────────────────────────────────────────┐
│  QUALITY GATE: dbt tests run after Silver ETL                │
└──────────────────────────────────────────────────────────────┘

  ┌─────────────────┐
  │  dbt test       │ ← Run automated data quality checks
  └────────┬────────┘
           │
           ▼
  ┌─────────────────────────────────────────────────────────┐
  │                   TESTS RUN                             │
  │                                                         │
  │  ✓ Schema tests: Are all required columns present?      │
  │  ✓ Not null: Do critical fields have values?            │
  │  ✓ Unique: No duplicate primary keys?                   │
  │  ✓ Custom: Coordinates within valid ranges?             │
  └────────┬────────────────────────────────────────────────┘
           │
           ├─ All pass → Proceed to Gold
           │
           └─ Any fail → Pipeline stops, alerts you
```

**Key concept: Quality Gate**
- Like a checkpoint in a video game
- Data must pass tests before moving to the next stage
- Prevents bad data from reaching dashboards

### Stage 4: Gold Models (dbt Transformations)

```
┌──────────────────────────────────────────────────────────────┐
│  GOLD: What happens when you run `just gold`                 │
└──────────────────────────────────────────────────────────────┘

  Silver Iceberg table (telemetry_fixes)
           │
           ▼
  ┌─────────────────────────────────────────────────────────┐
  │                 dbt SQL MODELS                          │
  │                                                         │
  │  pace_summary_by_round.sql                              │
  │    → Calculates average pace per round                  │
  │                                                         │
  │  signal_quality_rounds.sql                              │
  │    → Counts GPS signal quality per round                │
  │                                                         │
  │  device_health_errors.sql                               │
  │    → Tracks device errors and battery issues            │
  └────────┬────────────────────────────────────────────────┘
           │
           ▼
  Gold Iceberg tables (ready for dashboards!)
```

**Key concept: Pre-aggregation**
- Gold tables are summaries, not raw data
- Queries run fast because heavy lifting is done in advance
- Business users get instant answers

---

## Available Commands

| Command | Description | When to Use |
|---------|-------------|-------------|
| `just up` | Start all Docker services | Beginning of your work session |
| `just down` | Stop all services | End of work, or to reset |
| `just ui` | Show service URLs | When you forget which port is which |
| `just status` | Show recent DAG run status | To check if pipelines succeeded |
| `just bronze-upload <course> <path> [date]` | Upload single file | Testing one file |
| `just bronze-upload-all [date]` | Upload ALL CSVs | Bulk ingestion |
| `just silver <course> <date> <prefix>` | Process single course | Testing transformations |
| `just silver-all [date]` | Process ALL courses | Production run |
| `just gold` | Build Gold dbt models | After Silver succeeds |
| `just pipeline-all [date]` | Full pipeline (Bronze → Silver → Gold) | End-to-end run |
| `just logs` | Follow service logs | Debugging issues |
| `just reset-local` | ⚠️ Wipe all local data | Start fresh (destructive!) |

### Ingestion Tracking Commands

| Command | Description | When to Use |
|---------|-------------|-------------|
| `just registry-init` | Create ingestion_log table | Once after first `just up` |
| `just ingestion-status [date]` | Show what was ingested on a date | Daily check |
| `just ingestion-summary [date]` | Show counts by layer/status | Quick overview |
| `just ingestion-missing [layer]` | Show courses not ingested today | Catch missed files |
| `just ingestion-history <course>` | Show full history for a course | Debugging |

---

## Ingestion Tracking (Registry)

The **Ingestion Registry** tracks every file ingestion to:
1. **Prevent duplicates** - Don't re-ingest files already processed
2. **Detect missing data** - Find courses that weren't ingested today
3. **Audit trail** - See complete history of what was processed and when

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION WITH TRACKING                      │
└─────────────────────────────────────────────────────────────────┘

  data/americanfalls.rounds.csv
           │
           ▼
  ┌─────────────────────┐
  │  1. CHECK REGISTRY  │ ← Query: "Has this file been ingested?"
  └──────────┬──────────┘
             │
      ┌──────┴──────┐
      │             │
      ▼             ▼
  ┌───────┐    ┌────────┐
  │ EXISTS│    │  NEW   │
  │(skip) │    │(ingest)│
  └───────┘    └────┬───┘
                    │
                    ▼
           ┌─────────────────┐
           │  2. UPLOAD TO   │
           │     BRONZE      │
           └────────┬────────┘
                    │
                    ▼
           ┌─────────────────┐
           │  3. REGISTER    │ ← INSERT into ingestion_log
           │   SUCCESS       │
           └─────────────────┘
```

### First-Time Setup

After starting the stack for the first time, initialize the registry:

```bash
just up           # Start services
just registry-init  # Create the ingestion_log table
```

### Check What's Been Ingested

```bash
# Show today's ingestions
just ingestion-status

# Show a specific date
just ingestion-status 2025-12-16

# Quick summary (counts by status)
just ingestion-summary
```

Example output:
```
📋 INGESTION STATUS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Date: 2025-12-16

 course_id         | layer  | status  | rows | secs | completed
-------------------+--------+---------+------+------+-----------
 americanfalls     | bronze | success | 2949 |  1.2 | 14:23:15
 erinhills         | bronze | success | 3471 |  1.5 | 14:23:12
 indiancreek       | bronze | success | 1684 |  0.8 | 14:23:09
```

### Detect Missing Ingestions

Find courses that were ingested before but not today:

```bash
just ingestion-missing bronze
```

Example output:
```
⚠️  MISSING INGESTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Layer: bronze
  (Courses ingested before but not today)

 course_id    | last_ingested
--------------+---------------
 pinehurst4   | 2025-12-15
```

### View Course History

See complete ingestion history for a specific course:

```bash
just ingestion-history americanfalls
```

### Registry Table Schema

The `ingestion_log` table stores:

| Column | Type | Description |
|--------|------|-------------|
| `filename` | VARCHAR | Source file name |
| `course_id` | VARCHAR | Golf course identifier |
| `ingest_date` | DATE | Date of ingestion |
| `layer` | VARCHAR | bronze, silver, or gold |
| `status` | VARCHAR | success, failed, skipped, running |
| `rows_processed` | INTEGER | Number of rows processed |
| `file_hash` | VARCHAR | MD5 hash for change detection |
| `s3_path` | VARCHAR | Where the file was stored |
| `duration_seconds` | NUMERIC | How long it took |
| `error_message` | TEXT | Error details if failed |

---

## Key Features

### Idempotency (Safe to Re-run)

**What it means:** Running a command multiple times produces the same result without side effects.

```
# First run:
just bronze-upload-all 2025-12-16
# Output: ✓  DONE: 2,951 rows → s3://...

# Second run (same command):
just bronze-upload-all 2025-12-16
# Output: ⏭  SKIP: Already exists → s3://...
```

**Why it matters:**
- Safe to retry after failures
- No duplicate data
- No manual cleanup needed

### Resumability

**What it means:** If a pipeline fails mid-way, you can restart from where it left off.

**How it works:**
- All DAGs have `retries: 1` (automatic retry on failure)
- Iceberg writes use **APPEND mode** with delete-then-insert for idempotency
- Airflow tracks task state

### Append Mode for Scale

**Why we use APPEND instead of createOrReplace:**

| Mode              | Behavior                        | Use Case               |
|-------------------|--------------------------------|------------------------|
| `createOrReplace` | Overwrites entire table        | Small PoC, single run  |
| **`append`**      | Adds rows, keeps existing data | Production (650 courses!) |

**Our pattern (idempotent append):**
```sql
-- First: Delete existing data for this course/date
DELETE FROM silver.fact_telemetry_event 
WHERE course_id = 'americanfalls' AND ingest_date = '2025-12-16';

-- Then: Append new data
INSERT INTO silver.fact_telemetry_event SELECT * FROM new_data;
```

This allows:
- ✅ Safe re-runs (delete old, insert new)
- ✅ Incremental backfill (7 years of data, one course at a time)
- ✅ Partition-level updates (update one course without touching others)

---

## 📊 Viewing and Querying Data

### Option 1: Trino CLI (Recommended)

Connect to the SQL engine and run queries:

```bash
# Connect to Trino
docker exec -it trino trino

# Inside Trino, run SQL:
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;

-- Count rows by course
SELECT course_id, COUNT(*) as fixes 
FROM iceberg.silver.fact_telemetry_event 
GROUP BY course_id;

-- View sample data
SELECT round_id, course_id, fix_timestamp, latitude, longitude
FROM iceberg.silver.fact_telemetry_event 
LIMIT 10;

-- Query with date filter
SELECT * FROM iceberg.silver.fact_telemetry_event 
WHERE event_date = DATE '2025-12-16'
AND course_id = 'americanfalls';
```

### Option 2: Just Command (Quick Queries)

```bash
# Add to Justfile for convenience:
just query "SELECT COUNT(*) FROM iceberg.silver.fact_telemetry_event"
```

### Option 3: MinIO Browser (View Raw Files)

Navigate to these URLs to see the underlying Parquet/Iceberg files:

| Layer   | URL                                                                 |
|---------|---------------------------------------------------------------------|
| Bronze  | http://localhost:9001/browser/tm-lakehouse-landing-zone/            |
| Silver  | http://localhost:9001/browser/tm-lakehouse-source-store/warehouse/silver/ |
| Gold    | http://localhost:9001/browser/tm-lakehouse-serve/warehouse/gold/    |

**Note:** Silver/Gold data is in Iceberg format (Parquet files + metadata). You can see the directory structure but to query the data, use Trino.

### Observability (Seeing What Happened)

**Run Summaries:** Each stage writes a summary to the observability bucket:
```
s3://tm-lakehouse-observability/observability/runs/<stage>/run_id=<run_id>.json
```

**dbt Artifacts:** Test results are stored for auditing:
```
s3://tm-lakehouse-observability/observability/dbt_artifacts/run_id=<run_id>/
```

**Quarantine:** Invalid rows are preserved for investigation:
```
s3://tm-lakehouse-quarantine/observability/silver_quarantine/course_id=.../
```

---

## Troubleshooting

### Common Issues

#### "File already exists" message
✅ **This is normal!** Idempotency is working. The file was already uploaded, so it's skipped.

#### DAG shows as "queued" but not running
1. Check scheduler is running:
   ```bash
   docker logs airflow | grep scheduler
   ```
2. Look for errors:
   ```bash
   docker logs airflow | grep ERROR
   ```

#### Spark job fails
1. Check Spark is running:
   ```bash
   docker ps | grep spark
   ```
2. View Spark logs:
   ```bash
   docker logs spark
   ```
3. Verify Bronze file exists in MinIO UI: http://localhost:9001

#### dbt tests fail
- Check test logs in Airflow task output
- Review `run_results.json` in observability bucket
- Common causes:
  - Missing columns (check Silver schema)
  - Data quality threshold exceeded

#### Services won't start
1. Ensure Docker is running:
   ```bash
   docker ps
   ```
2. Check if ports are in use:
   ```bash
   lsof -i :8080  # Airflow
   lsof -i :9001  # MinIO
   ```
3. View logs:
   ```bash
   just logs
   ```

---

## Prerequisites

- **Docker Desktop** installed and running
- **`just`** (recommended for better UX):
  - macOS: `brew install just`
  - Or use `docker compose` commands directly

---

## Data Location

Your CSV files should be in the `data/` folder:
```
data/
  ├── americanfalls.rounds.csv
  ├── bradshawfarmgcjune.rounds_0601_0715.csv
  ├── erinhills.rounds.csv
  └── ...
```

These are automatically mounted into containers at `/opt/tagmarshal/input/`.

---

## Entity Relationship Diagram (ERD)

This diagram shows how data flows through the layers and the structure of key tables:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ENTITY RELATIONSHIP DIAGRAM                           │
└─────────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────────┐
                              │     RAW CSV         │
                              │   (Bronze Layer)    │
                              ├─────────────────────┤
                              │ • roundId           │
                              │ • course            │
                              │ • holes[]           │
                              │ • locations[]       │
                              │   └─ fixCoordinates │
                              │   └─ fixTime        │
                              │   └─ speed          │
                              └──────────┬──────────┘
                                         │
                          Spark ETL      │      (Transform & Validate)
                          ───────────────┼───────────────
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                      silver.telemetry_fixes                                │  │
│  │                         (Silver Layer)                                     │  │
│  ├────────────────────────────────────────────────────────────────────────────┤  │
│  │  PK │ round_id          VARCHAR    │ Unique identifier for the round      │  │
│  │     │ fix_timestamp     TIMESTAMP  │ When the GPS fix was recorded        │  │
│  │ ────┼───────────────────────────────────────────────────────────────────── │  │
│  │     │ course_id         VARCHAR    │ Golf course identifier               │  │
│  │     │ fix_latitude      DOUBLE     │ GPS latitude (-90 to 90)             │  │
│  │     │ fix_longitude     DOUBLE     │ GPS longitude (-180 to 180)          │  │
│  │     │ speed             DOUBLE     │ Speed at time of fix (m/s)           │  │
│  │     │ signal_quality    VARCHAR    │ GPS signal quality indicator         │  │
│  │     │ battery_level     INTEGER    │ Device battery percentage            │  │
│  │     │ device_id         VARCHAR    │ Identifier of tracking device        │  │
│  │     │ hole_number       INTEGER    │ Current hole being played            │  │
│  │     │ ingest_date       DATE       │ When data was ingested (partition)   │  │
│  │     │ event_date        DATE       │ When event occurred (partition)      │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│     Partitioned by: course_id, event_date                                        │
│     Format: Apache Iceberg                                                       │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                                         │
                         dbt Transform   │     (Aggregate & Summarize)
                         ────────────────┼────────────────
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
              ▼                          ▼                          ▼
┌─────────────────────────┐ ┌─────────────────────────┐ ┌─────────────────────────┐
│ gold.pace_summary       │ │ gold.signal_quality     │ │ gold.device_health      │
│     _by_round           │ │     _rounds             │ │     _errors             │
├─────────────────────────┤ ├─────────────────────────┤ ├─────────────────────────┤
│ round_id        VARCHAR │ │ round_id        VARCHAR │ │ device_id       VARCHAR │
│ course_id       VARCHAR │ │ course_id       VARCHAR │ │ course_id       VARCHAR │
│ avg_pace_seconds DOUBLE │ │ total_fixes     INTEGER │ │ error_count     INTEGER │
│ total_distance   DOUBLE │ │ good_signal_pct  DOUBLE │ │ low_battery_cnt INTEGER │
│ round_duration   DOUBLE │ │ poor_signal_pct  DOUBLE │ │ last_seen     TIMESTAMP │
│ holes_played    INTEGER │ │ no_signal_pct    DOUBLE │ │ status          VARCHAR │
└─────────────────────────┘ └─────────────────────────┘ └─────────────────────────┘

                                    │
                                    ▼
                        ┌───────────────────────┐
                        │     DASHBOARDS        │
                        │   (Downstream Use)    │
                        ├───────────────────────┤
                        │ • Pace of Play Report │
                        │ • Signal Quality Map  │
                        │ • Device Fleet Health │
                        └───────────────────────┘


─────────────────────────────────────────────────────────────────────────────────

                              DATA FLOW SUMMARY

  ┌─────────┐      ┌─────────┐      ┌─────────┐      ┌──────────────┐
  │  CSV    │ ───► │ BRONZE  │ ───► │ SILVER  │ ───► │    GOLD      │
  │ (Raw)   │      │ (Land)  │      │ (Clean) │      │ (Analytics)  │
  └─────────┘      └─────────┘      └─────────┘      └──────────────┘
       │                │                │                   │
       │                │                │                   │
    Ingest          Validate         Transform           Aggregate
    as-is           schema           & enrich            & serve

─────────────────────────────────────────────────────────────────────────────────
```

---

## Glossary

| Term | Definition |
|------|------------|
| **DAG** | Directed Acyclic Graph - a workflow of tasks with dependencies (used in Airflow) |
| **dbt** | Data Build Tool - a SQL-based transformation tool that applies software engineering practices to data |
| **ETL** | Extract, Transform, Load - the process of moving data from source to destination with modifications |
| **Iceberg** | An open table format for large datasets that provides ACID transactions, time-travel, and schema evolution |
| **Idempotent** | An operation that produces the same result no matter how many times you run it |
| **Lakehouse** | Architecture combining cheap data lake storage with data warehouse query capabilities |
| **Medallion** | A layered data architecture pattern (Bronze → Silver → Gold) |
| **MinIO** | Open-source S3-compatible object storage for local development |
| **Orchestration** | Coordinating and scheduling multiple tasks/jobs in the correct order |
| **Partition** | Dividing a table into smaller chunks based on column values for faster queries |
| **Quarantine** | A separate storage location for invalid/problematic data rows |
| **Quality Gate** | A checkpoint where data must pass automated tests before proceeding |
| **S3** | Amazon Simple Storage Service - object storage in AWS (MinIO emulates this locally) |
| **Schema** | The structure of a table (column names, data types, constraints) |
| **Spark** | Distributed computing engine for large-scale data processing |
| **Time-travel** | Iceberg feature that lets you query data as it existed at a past point in time |
| **Trino** | Distributed SQL query engine (formerly called Presto) |

---

## Next Steps

Once you're comfortable with running the pipeline:

1. **Explore the data**: Use Trino to query Silver/Gold tables
2. **Add new dbt models**: Create custom aggregations in `transform/dbt_project/models/gold/`
3. **Review dbt tests**: See how quality gates work in `transform/dbt_project/models/*/schema.yml`
4. **Check observability**: Look at run summaries in MinIO to understand what happened
