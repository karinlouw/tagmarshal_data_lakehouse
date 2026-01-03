# Pipeline Walkthrough - Step by Step

> **Working Document**: We update this as we run each pipeline step together.
> 
> **Goal**: Understand exactly what happens at each stage, which files run, and how data flows through the system.

---

## Quick Reference

| Step | Command | What Happens | Status |
|------|---------|--------------|--------|
| 1 | `just up` | Start all Docker services | ✅ |
| 2 | `just bronze-upload-all` | Upload CSV/JSON to Bronze (MinIO) | ✅ |
| 3 | `just silver-all` | Transform Bronze → Silver (Iceberg) | ✅ |
| 4 | `just gold` | Build analytics tables with dbt | ✅ |
| 5 | `just trino` | Query results in Trino | ✅ |

**Full Pipeline (all steps at once):**
```bash
just pipeline-all
```

---

## Step 1: Start the Docker Stack ✅

### Command
```bash
just up
```

### What It Does
Starts 7 Docker containers that make up our lakehouse:

| Container | Purpose | Port |
|-----------|---------|------|
| `minio` | S3-compatible storage (our "data lake") | 9000, 9001 |
| `minio-init` | Creates buckets on startup | (exits after setup) |
| `iceberg-rest` | Table catalog - tracks where data lives | 8181 |
| `spark` | ETL engine for transformations | 7077, 8082 |
| `spark-worker` | Spark worker node | - |
| `trino` | SQL query engine | 8081 |
| `airflow` | Orchestration - runs our DAGs | 8080 |
| `airflow-postgres` | Airflow's metadata database | 5432 |

### Files Involved
```
docker-compose.yml          # Defines all containers
config/local.env            # Environment variables
```

### How to Verify
```bash
# Check all containers are running
docker ps

# Check Airflow is ready
curl -s http://localhost:8080/health
```

### URLs to Bookmark
- **Airflow**: http://localhost:8080 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Trino**: http://localhost:8081
- **Spark Master**: http://localhost:8082

---

## Step 2: Bronze Layer - Upload Data ✅

> **Status**: ✅ Complete

### Commands

**Option A: Upload a single file**
```bash
just bronze-upload <course_id> <file_path>

# Examples:
just bronze-upload indiancreek data/indiancreek.rounds.csv
just bronze-upload indiancreek data/indiancreek.rounds_json.json
```

**Option B: Upload ALL files in data/ folder**
```bash
just bronze-upload-all

# With custom date:
just bronze-upload-all 2026-01-03
```

This uploads all CSV and JSON files at once:

| File | Size | Course ID |
|------|------|-----------|
| americanfalls.rounds.csv | 13MB | americanfalls |
| bradshawfarmgc.rounds_0601_0715.csv | 60MB | bradshawfarmgc |
| bradshawfarmgc.rounds_0716_0831.csv | 66MB | bradshawfarmgc |
| erinhills.rounds.csv | 23MB | erinhills |
| indiancreek.rounds.csv | 10MB | indiancreek |
| indiancreek.rounds_json.json | 38MB | indiancreek |
| pinehurst4.rounds.csv | 22MB | pinehurst4 |

> **Note**: Multiple files with the same course_id (like bradshawfarmgc) are stored separately - the filename is part of the S3 key, so they don't overwrite each other.

### What It Does
1. Validates the file (checks structure, counts rows)
2. Uploads to MinIO bucket: `tm-lakehouse-landing-zone`
3. Records the upload in the ingestion registry (PostgreSQL)

### View the Ingestion Registry
```bash
# View today's ingestion status
just ingestion-status

# View status for a specific date
just ingestion-status 2026-01-03

# Clear registry for a date (if you need to re-run)
just registry-clear 2026-01-03
```

> **Note**: The registry is auto-initialized when PostgreSQL first starts (via `docker-entrypoint-initdb.d`). On existing setups, run `just registry-init` once.

### Files That Run
```
jobs/spark/lib/tm_lakehouse/bronze.py   # Main logic (186 lines)
  ├── detect_file_format()              # CSV or JSON? (line 33)
  ├── validate_csv_header()             # Check CSV headers (line 53)
  ├── validate_json_structure()         # Check JSON structure (line 66)
  └── upload_file_to_bronze()           # Upload to MinIO (line 118)

orchestration/airflow/dags/bronze_ingest_dag.py  # Airflow DAG (149 lines)
  └── bronze_upload_task()              # Main task function (line 24)
```

### Data Flow
```
Local File (data/indiancreek.rounds.csv)
    ↓
Airflow DAG triggered (bronze_ingest)
    ↓
Validation (check _id, course, locations[0].startTime)
    ↓
MinIO: s3://tm-lakehouse-landing-zone/course_id=indiancreek/ingest_date=2026-01-03/indiancreek.rounds.csv
    ↓
Registry: PostgreSQL ingestion_log table
```

### Our Upload Result
```
course_id=indiancreek/ingest_date=2026-01-03/indiancreek.rounds.csv (10,975,831 bytes)
```

### How to Verify
```bash
# Check MinIO for uploaded file (via Airflow container)
docker exec airflow python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
for obj in s3.list_objects_v2(Bucket='tm-lakehouse-landing-zone', 
    Prefix='course_id=indiancreek').get('Contents', []):
    print(f'{obj[\"Key\"]} ({obj[\"Size\"]:,} bytes)')
"

# Check pipeline status
just status
```

### Bug We Fixed
The `just bronze-upload` command was passing `data/file.csv` directly, but inside the Docker container the data is mounted at `/opt/tagmarshal/input/`. Fixed by converting the path automatically.

---

## Understanding: How Deduplication Works

### The Challenge
We have files like:
- `bradshawfarmgc.rounds_0601_0715.csv` (June data)
- `bradshawfarmgc.rounds_0716_0831.csv` (July data)

Both are the **same course** (`Bradshaw Farms`) but different time periods. How do we:
1. Avoid duplicates when re-running backfills?
2. Allow daily incremental uploads?
3. Keep historical data intact?

### The Solution: Three Layers of Protection

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: BRONZE (Landing Zone)                             │
│  ─────────────────────────────────────────────────────────  │
│  Files stored by: course_id + ingest_date + filename        │
│                                                             │
│  s3://landing-zone/                                         │
│    course_id=bradshawfarmgc/                                │
│      ingest_date=2026-01-03/                                │
│        bradshawfarmgc.rounds_0601_0715.csv  ✓               │
│        bradshawfarmgc.rounds_0716_0831.csv  ✓               │
│                                                             │
│  → Same file uploaded twice? SKIPPED (idempotent)           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2: INGESTION REGISTRY (PostgreSQL)                   │
│  ─────────────────────────────────────────────────────────  │
│  Tracks: filename + ingest_date + stage + file_hash         │
│                                                             │
│  → Already ingested? SKIPPED before upload even starts      │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3: SILVER (Iceberg Table)                            │
│  ─────────────────────────────────────────────────────────  │
│  Deduplication: round_id + fix_timestamp                    │
│                                                             │
│  Before APPEND:                                             │
│    DELETE FROM silver.fact_telemetry_event                  │
│    WHERE course_id = 'bradshawfarmgc'                       │
│      AND ingest_date = '2026-01-03'                         │
│                                                             │
│  → Re-running Silver ETL? Old data DELETED first, then new  │
│    data APPENDED (idempotent)                               │
└─────────────────────────────────────────────────────────────┘
```

### Key Points

1. **Single Table for All Courses**: We DON'T create separate tables per course. All data goes into `silver.fact_telemetry_event`, partitioned by `course_id` and `event_date`.

2. **Natural Deduplication**: Each round has a unique `_id` from MongoDB. Within a round, each location record has a unique `fix_timestamp`. The combination `(round_id, fix_timestamp)` is unique.

# Todo: make sure to check row counts and file size to determine file already uploaded
3. **Idempotent Re-runs**: You can safely run:
   - `just bronze-upload-all` multiple times → files already uploaded are skipped
   - `just silver-all` multiple times → old data deleted before new data appended

4. **Backfill + Daily Updates**:
   ```bash
   # Initial backfill (run once)
   just bronze-upload-all 2026-01-01
   just silver-all 2026-01-01
   
   # Daily updates (run each day)
   just bronze-upload-all 2026-01-02
   just silver-all 2026-01-02
   ```

### Relevant Code

**Silver ETL deduplication** (`jobs/spark/silver_etl.py` lines 373-389):

```python
# Delete existing data for this course/ingest_date (idempotency)
spark.sql(f"""
    DELETE FROM {table} 
    WHERE course_id = '{args.course_id}' 
    AND ingest_date = '{args.ingest_date}'
""")

# Then append new data
valid.writeTo(table).append()
```

**Within-batch deduplication** (`jobs/spark/silver_etl.py` lines 327-335):

```python
# Dedup: prefer is_cache=true for same (round_id, fix_timestamp)
w = Window.partitionBy("round_id", "fix_timestamp").orderBy(
    F.col("is_cache").desc_nulls_last()
)
out = out.withColumn("_rn", F.row_number().over(w))
         .filter(F.col("_rn") == 1)
         .drop("_rn")
```

---

## Step 3: Silver Layer - Transform Data ✅

> **Status**: ✅ Complete

### Command
```bash
# Process all courses at once
just silver-all

# Or process a single course
just silver <course_id> [date]

# Examples:
just silver indiancreek              # Process ALL dates for indiancreek
just silver indiancreek 2026-01-03   # Process only this specific date
just silver bradshawfarmgc           # Process ALL dates for bradshawfarmgc
```

- **No date**: Scans MinIO and processes ALL available dates for that course
- **With date**: Processes only that specific date

### What It Does
1. Reads Bronze files from MinIO
2. **Explodes** the `locations` array → 1 row per location (so we can query/aggregate individual GPS fixes)
3. Writes to Iceberg table in Silver layer

### Files That Run
```
jobs/spark/silver_etl.py                # Main ETL script
  ├── detect_file_format()              # CSV or JSON?
  ├── discover_location_indices()       # Find location columns (CSV)
  ├── process CSV or JSON               # Different paths
  └── write to Iceberg                  # Save to Silver

jobs/spark/lib/tm_lakehouse/
  ├── config.py                         # Configuration
  ├── iceberg.py                        # Iceberg helpers
  └── spark_session.py                  # Spark setup
```

### Data Transformation
```
Bronze (1 row = 1 round with nested locations)
    ↓
Silver (1 row = 1 location record per round)

Example:
  Bronze: 1 row  → round_123 with 55 nested locations
  Silver: 55 rows → round_id=123, location_index=0, 1, 2, ... 54
```

### Key Silver Columns
| Column | Description |
|--------|-------------|
| `round_id` | Unique round identifier |
| `course_id` | Which golf course |
| `hole_number` | Current hole (1-18+) |
| `nine_number` | Which nine (1 or 2) |
| `pace_of_play` | Seconds for this section |
| `fix_timestamp` | When recorded |

### Our Silver Result
```
┌─────────────────┬─────────┐
│ course_id       │ rows    │
├─────────────────┼─────────┤
│ bradshawfarmgc  │ 466,634 │
│ erinhills       │ 148,198 │
│ pinehurst4      │ 137,837 │
│ americanfalls   │  75,000 │
│ indiancreek     │  67,660 │
├─────────────────┼─────────┤
│ TOTAL           │ 895,329 │
└─────────────────┴─────────┘
```

### How to Verify
```bash
# Check row counts by course
docker exec trino trino --execute "
  SELECT course_id, count(*) as rows 
  FROM iceberg.silver.fact_telemetry_event 
  GROUP BY course_id
"
```

---

## Step 4: Gold Layer - Analytics with dbt ✅

> **Status**: ✅ Complete

### Command
```bash
just gold
```

### What It Does
1. Installs dbt-trino inside Airflow container
2. Runs dbt models that query Silver Iceberg tables via Trino
3. Creates pre-aggregated analytics tables in Gold schema

### Files That Run
```
transform/dbt_project/
  ├── dbt_project.yml             # dbt configuration
  ├── profiles.yml                # Trino connection settings
  ├── models/
  │   ├── sources.yml             # Defines Silver as source
  │   └── gold/
  │       ├── pace_summary_by_round.sql     # Round-level pace metrics
  │       ├── signal_quality_rounds.sql     # Signal quality per round
  │       └── device_health_errors.sql      # Battery/health issues
  └── packages.yml                # dbt_utils dependency

orchestration/airflow/dags/gold_dbt_dag.py  # Airflow DAG (70 lines)
```

### Gold Models

| Model | Description | Use Case |
|-------|-------------|----------|
| `pace_summary_by_round` | Round-level pace aggregates | "Which rounds were slow?" |
| `signal_quality_rounds` | Projected/problem fix rates | "Which rounds had GPS issues?" |
| `device_health_errors` | Low battery events | "Which devices need charging?" |

### Our Gold Results

---

#### 1. Pace Summary by Round

**What it shows:** Average pace (in seconds) and round duration for each course.

```
┌─────────────────┬────────┬───────────┬─────────────┬───────────┐
│ course_id       │ rounds │ avg_pace  │ avg_dur_min │ avg_fixes │
├─────────────────┼────────┼───────────┼─────────────┼───────────┤
│ bradshawfarmgc  │ 16,437 │ +367.5s   │ 153 min     │ 57        │
│ erinhills       │  3,471 │  +62.4s   │ 268 min     │ 43        │
│ pinehurst4      │  3,010 │ -256.2s   │ 263 min     │ 46        │
│ americanfalls   │  2,949 │  -84.3s   │ 162 min     │ 25        │
│ indiancreek     │  1,684 │ +373.7s   │ 174 min     │ 40        │
└─────────────────┴────────┴───────────┴─────────────┴───────────┘
```

**Key Insights:**
- **Positive pace** = behind schedule, **Negative pace** = ahead of schedule
- **Erin Hills** has longest avg round (268 min / 4.5 hrs) - championship course
- **American Falls** (9-hole) has only 25 fixes/round vs 57 for Bradshaw Farm (27-hole)
- **Pinehurst 4** players are fastest (-256s ahead of goal time on average)

**SQL Query (run in DBeaver):**
```sql
-- Pace Summary with Round Duration
SELECT 
    course_id,
    count(*) as rounds,
    round(avg(avg_pace), 1) as avg_pace_sec,
    round(avg(date_diff('minute', round_start_ts, round_end_ts)), 0) as avg_duration_min,
    round(avg(fix_count), 0) as avg_fixes,
    round(min(avg_pace), 0) as fastest_pace,
    round(max(avg_pace), 0) as slowest_pace
FROM iceberg.gold.pace_summary_by_round
WHERE round_end_ts > round_start_ts
GROUP BY course_id
ORDER BY rounds DESC;
```

---

#### 2. Device Health Issues by Course

**What it shows:** Battery issues as percentage of each course's total GPS events.

```
┌───────────────┬──────────────┬───────────────┬─────────────────┐
│ course_id     │ total_events │ health_issues │ % with issues   │
├───────────────┼──────────────┼───────────────┼─────────────────┤
│ americanfalls │ 75,000       │ 38,689        │ 51.6% ⚠️        │
│ pinehurst4    │ 137,837      │ 3,464         │ 2.5%            │
│ erinhills     │ 148,198      │ 1,416         │ 1.0%            │
│ indiancreek   │ 67,660       │ 327           │ 0.5%            │
│ bradshawfarmgc│ 933,268      │ 0             │ 0.0% ✅         │
└───────────────┴──────────────┴───────────────┴─────────────────┘
```

**Key Insights:**
- **American Falls has a serious battery problem!** 51.6% of events have low/critical battery
- **Bradshaw Farm** has zero battery issues - excellent device management
- This suggests American Falls devices may need replacement or more frequent charging

**SQL Query (run in DBeaver):**
```sql
-- Device Health as % of Course Events
WITH course_totals AS (
    SELECT course_id, count(*) as total_events
    FROM iceberg.silver.fact_telemetry_event
    GROUP BY course_id
),
health_issues AS (
    SELECT course_id, count(*) as health_events
    FROM iceberg.gold.device_health_errors
    GROUP BY course_id
)
SELECT 
    c.course_id,
    c.total_events,
    coalesce(h.health_events, 0) as health_issues,
    round(coalesce(h.health_events, 0) * 100.0 / c.total_events, 1) as pct_with_issues
FROM course_totals c
LEFT JOIN health_issues h ON c.course_id = h.course_id
ORDER BY pct_with_issues DESC;
```

---

#### 3. Battery Issue Breakdown

**What it shows:** Critical vs Low battery events across all courses.

```
┌───────────────┬──────────────────┬────────┬─────────┐
│ course_id     │ health_flag      │ events │ % total │
├───────────────┼──────────────────┼────────┼─────────┤
│ americanfalls │ battery_critical │ 38,573 │  87.9%  │
│ pinehurst4    │ battery_critical │  1,799 │   4.1%  │
│ pinehurst4    │ battery_low      │  1,665 │   3.8%  │
│ erinhills     │ battery_critical │    802 │   1.8%  │
│ erinhills     │ battery_low      │    614 │   1.4%  │
└───────────────┴──────────────────┴────────┴─────────┘
```

**Key Insights:**
- **87.9%** of ALL battery issues across ALL courses are from American Falls
- Most issues are `battery_critical` (<10%) rather than `battery_low` (<20%)

**SQL Query (run in DBeaver):**
```sql
-- Battery Issues by Type and Course
SELECT 
    course_id, 
    health_flag, 
    count(*) as events,
    round(count(*) * 100.0 / sum(count(*)) over(), 1) as pct_of_total
FROM iceberg.gold.device_health_errors
GROUP BY course_id, health_flag
ORDER BY events DESC;
```

### How to Verify
```bash
# List Gold tables
just trino-query "SHOW TABLES IN iceberg.gold"

# Query pace summary
just trino-query "SELECT * FROM iceberg.gold.pace_summary_by_round LIMIT 5"

# Check device health
just trino-query "SELECT health_flag, count(*) FROM iceberg.gold.device_health_errors GROUP BY 1"
```

### Data Flow
```
Silver (iceberg.silver.fact_telemetry_event)
    ↓
dbt models (SQL transformations)
    ↓
Gold (iceberg.gold.*)
    ├── pace_summary_by_round      # 27,551 rows
    ├── signal_quality_rounds      # 27,551 rows
    └── device_health_errors       # 43,896 rows
```

---

## Step 5: Query Results ✅

> **Status**: ✅ Complete

### Commands
```bash
# Interactive Trino shell
just trino

# Quick query
just trino-query "SELECT count(*) FROM iceberg.silver.fact_telemetry_event"

# Check Trino health
just trino-status
```

### Example Queries

**Silver Layer - Detailed Event Data**
```sql
-- Count rounds per course
SELECT course_id, count(DISTINCT round_id) as rounds
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id;

-- Average pace by hole at Bradshaw Farm
SELECT hole_number, nine_number, round(avg(pace), 0) as avg_pace
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'bradshawfarmgc'
GROUP BY hole_number, nine_number
ORDER BY nine_number, hole_number;

-- Which nines are most popular at 27-hole course?
SELECT 
    array_agg(DISTINCT nine_number ORDER BY nine_number) as nines_played,
    count(DISTINCT round_id) as rounds
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'bradshawfarmgc'
GROUP BY round_id
HAVING count(DISTINCT nine_number) >= 2;
```

**Gold Layer - Pre-Aggregated Analytics**
```sql
-- Slowest courses by average pace
SELECT course_id, round(avg(avg_pace), 0) as overall_pace
FROM iceberg.gold.pace_summary_by_round
GROUP BY course_id
ORDER BY overall_pace DESC;

-- Courses with battery issues
SELECT course_id, health_flag, count(*) as events
FROM iceberg.gold.device_health_errors
GROUP BY course_id, health_flag
ORDER BY events DESC;
```

### Our Query Results
```
┌─────────────────┬────────┬─────────────┐
│ Layer           │ Rows   │ Description │
├─────────────────┼────────┼─────────────┤
│ Silver          │ 1.36M  │ GPS events  │
│ Gold (pace)     │ 27,551 │ Round stats │
│ Gold (quality)  │ 27,551 │ Signal QA   │
│ Gold (health)   │ 43,896 │ Battery low │
└─────────────────┴────────┴─────────────┘
```

---

## Troubleshooting

### Check Container Logs
```bash
docker logs airflow --tail 50
docker logs spark --tail 50
```

### Restart a Service
```bash
just restart-service airflow
```

### Start Fresh (keep data)
```bash
just down
just up
```

### Nuclear Option (delete ALL data)
```bash
just nuke      # Stops everything, deletes all volumes
just up        # Start fresh with empty databases
```

### DAGs Not Running?
DAGs now start **unpaused** by default. If you have issues:
```bash
# Check DAG status
docker exec airflow airflow dags list

# Manually unpause if needed
docker exec airflow airflow dags unpause bronze_ingest
```

---

## Progress Tracker

| Step | Status | Notes |
|------|--------|-------|
| 1. Start Stack | ✅ Complete | All services running |
| 2. Bronze Upload | ✅ Complete | 7 files → MinIO |
| 3. Silver ETL | ✅ Complete | 1.36M rows → Iceberg |
| 4. Gold dbt | ✅ Complete | 3 analytics tables created |
| 5. Query Results | ✅ Complete | Trino queries working |

---

## What's Next?

The pipeline is complete! For production use:

1. **Backfill Historical Data**: Use `just backfill-silver` for resumable bulk processing
2. **Daily Operations**: Run `just pipeline-all` for full Bronze → Silver → Gold
3. **Monitoring**: Check `just backfill-status` for ingestion tracking

📚 **See also:** [Command Reference](command_reference.md) - Complete list of all `just` commands

---

*Last updated: All 5 steps complete - Full pipeline working with Bronze, Silver, and Gold layers*

