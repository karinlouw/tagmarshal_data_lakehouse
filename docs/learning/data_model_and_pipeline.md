# Data Model & Pipeline Guide

This guide explains how data flows through the Tagmarshal lakehouse and what the data looks like at each stage.

---

## 🎯 The Big Picture

**What we're building:** A lakehouse that consolidates **processed round data** (round strings) from TagMarshal into analytics-ready tables.

> **Important:** We're working with **pre-processed data**, not raw GPS telemetry. TagMarshal's system has already:
> - Processed raw GPS pings into structured rounds
> - Calculated pace metrics (pace vs goal, gaps to other groups)
> - Assigned locations to holes and sections
> - Tracked device status (battery, signal quality)
>
> Our job is to ingest this processed data, transform it into a queryable format, and build analytics on top.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA FLOW                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   INPUT                    BRONZE              SILVER              GOLD  │
│   ─────                    ──────              ──────              ────  │
│                                                                          │
│   CSV/JSON files    →    Raw files in    →   Iceberg table   →   dbt    │
│   (or API)               MinIO/S3            (long format)        models │
│                                                                          │
│   1 row = 1 round         Same as           1 row = 1 GPS       Summary  │
│   with ~50 locations      input             fix (exploded)      tables   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 The Data Model

### Source Data: What We Get

Each record represents a **round** (one cart/group playing the course). It contains:

| Field | Description | Example |
|-------|-------------|---------|
| `_id` | Unique round identifier | `6724cb30657f3deb031720f3` |
| `course` | Course name | `indiancreek` |
| `device` | Tracker device ID | `68259d045b4523ced1f9ba66` |
| `startTime` | When round started | `2024-11-01T12:30:52.000Z` |
| `endTime` | When round ended | `2024-11-01T16:37:01.000Z` |
| `goalTime` | Target round time (seconds) | `14340` (4 hours) |
| `startHole` | Which hole the round started on | `1` or `10` (shotgun start) |
| `isNineHole` | Is this a 9-hole round? | `true` / `false` |
| `currentNine` | Which nine (front/back) | `1` or `2` |
| `complete` | Did the round finish? | `true` / `false` |
| `locations` | **Array of section records** | (see below) |

**Important course configurations:**
- **Shotgun starts:** Rounds can start on hole 1 OR hole 10
- **9-hole rounds:** Some players only play 9 holes (`isNineHole=true`)
- **9-hole courses played twice:** A player plays holes 1-9, then 1-9 again (tracked via `currentNine`)
- **Courses with 27+ holes:** Some courses have more than 18 holes (100+ location entries)

**Location count ranges (from pilot data):**
- Minimum: 18 (short 9-hole rounds)
- Maximum: 117 (27-hole courses or very long rounds)
- Average: ~40

### The `locations` Array (Most Important!)

Each round has **18-117 location entries** (average ~40). The number varies by:
- Course layout (18 holes = ~53 sections, 27 holes = more)
- Round completeness (incomplete rounds have fewer)
- 9-hole vs 18-hole rounds

Each location is a **processed section record** (TagMarshal has already processed the raw GPS pings):

```json
{
  "hole": 1,                  // Current hole (1-18)
  "holeSection": 2,           // Section within hole (1=tee, 2=fairway, 3=green)
  "sectionNumber": 2,         // Overall section (1-53 for 18 holes)
  "startTime": 300,           // Seconds from round start
  "fixCoordinates": [
    -80.1396557,              // Longitude
    25.8748464                // Latitude
  ],
  "pace": 0,                  // Current pace vs goal (negative = ahead, positive = behind)
  "paceGap": null,            // Time gap to group ahead (seconds)
  "positionalGap": null,      // Physical distance to group ahead
  "batteryPercentage": 100,   // Device battery level
  "isProjected": false,       // Was this location estimated?
  "isProblem": false,         // Was there a signal issue?
  "isCache": false            // Is this cached data?
}
```

**What the sections mean:**
- A typical 18-hole course has ~53 sections (3 sections per hole: tee, fairway, green)
- Each location record represents when the group entered a new section
- This is NOT every GPS ping - it's a processed summary of movement through the course

### Data Format Differences

**CSV Format** (flattened):
```csv
_id,course,locations[0].hole,locations[0].startTime,locations[0].fixCoordinates[0],...
6724cb30...,indiancreek,1,300,-80.1396557,...
```

**JSON Format** (nested - from API):
```json
{
  "_id": {"$oid": "6724cb30..."},
  "course": "indiancreek",
  "locations": [
    {"hole": 1, "startTime": 300, "fixCoordinates": [-80.1396557, 25.8748464]},
    ...
  ]
}
```

---

## 🗄️ Database Tables

### Silver: `fact_telemetry_event`

The Silver ETL **explodes** the locations array into one row per section record:

| Column | Type | Description |
|--------|------|-------------|
| **Round Identity** | | |
| `round_id` | STRING | Unique round identifier |
| `course_id` | STRING | Course identifier (partition key) |
| `ingest_date` | STRING | When data was ingested |
| `event_date` | DATE | Date of record (partition key) |
| `fix_timestamp` | TIMESTAMP | When location was recorded |
| **Round Configuration** | | |
| `start_hole` | INT | Which hole the round started on (1 or 10 for shotgun) |
| `start_section` | INT | Starting section number |
| `end_section` | INT | Ending section number |
| `is_nine_hole` | BOOLEAN | Is this a 9-hole round? |
| `current_nine` | INT | Which nine (1=front, 2=back) |
| `goal_time` | INT | Target round time in seconds |
| `is_complete` | BOOLEAN | Did the round complete? |
| **Location Fields** | | |
| `location_index` | INT | Position in locations array |
| `hole_number` | INT | Current hole (1-18) |
| `section_number` | INT | Overall section (1-53 for 18 holes) |
| `hole_section` | INT | Section within hole (1=tee, 2=fairway, 3=green) |
| `nine_number` | INT | Derived: 1 if hole ≤9, else 2 |
| `longitude` | DOUBLE | GPS longitude |
| `latitude` | DOUBLE | GPS latitude |
| `geometry_wkt` | STRING | `POINT(-80.139 25.874)` |
| **Pace Metrics** | | |
| `pace` | DOUBLE | Current pace vs goal (negative=ahead, positive=behind) |
| `pace_gap` | DOUBLE | Time gap to group ahead (seconds) |
| `positional_gap` | DOUBLE | Distance to group ahead |
| **Device Status** | | |
| `battery_percentage` | DOUBLE | Device battery level |
| `is_cache` | BOOLEAN | Was data cached? |
| `is_projected` | BOOLEAN | Was location estimated? |
| `is_problem` | BOOLEAN | Signal quality issue? |

**Example Query:**
```sql
SELECT 
  round_id,
  hole_number,
  fix_timestamp,
  pace,
  longitude,
  latitude
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'indiancreek'
  AND event_date = DATE '2024-11-01'
ORDER BY round_id, fix_timestamp;
```

### Gold Tables (Analytics-Ready)

| Table | Purpose | Key Metrics |
|-------|---------|-------------|
| `pace_summary_by_round` | Round-level pace analysis | avg_pace, total_time, pace_gap |
| `signal_quality_rounds` | Signal quality per round | problem_count, projected_count |
| `device_health_errors` | Device/battery tracking | avg_battery, low_battery_count |

---

## 🚀 Running the Pipeline

### Prerequisites

1. **Docker Desktop** running
2. **just** installed (`brew install just`)

### Step-by-Step

```bash
# 1. Start all services
just up

# 2. Wait for services to initialize (~30 seconds)
just health              # Check all containers are "Up"

# 3. Initialize the registry (first time only)
just registry-init

# 4. Run the full pipeline
just pipeline-all 2025-01-03

# Or run each stage manually:
just bronze-upload-all 2025-01-03   # Upload files to MinIO
just silver-all 2025-01-03          # Transform to Iceberg
just gold                           # Build dbt models
```

### What Each Stage Does

#### Bronze (Upload)
```
data/*.csv  ──────────────────>  MinIO
data/*.json                       └── tm-lakehouse-landing-zone/
                                       └── course_id=indiancreek/
                                            └── ingest_date=2025-01-03/
                                                 └── indiancreek.rounds.csv
```
- Copies files exactly as-is to object storage
- Organizes by course and date
- Idempotent: skips if file already exists

#### Silver (ETL)
```
MinIO (raw files)  ──>  Spark  ──>  Iceberg Table
                        │           └── silver.fact_telemetry_event
                        │
                        ├── Explode locations array (1 row → 50 rows)
                        ├── Normalize MongoDB JSON types
                        ├── Validate coordinates
                        ├── Deduplicate GPS fixes
                        └── Quarantine invalid rows
```

#### Gold (dbt)
```
Iceberg (Silver)  ──>  dbt/Trino  ──>  Iceberg (Gold)
                                        ├── pace_summary_by_round
                                        ├── signal_quality_rounds
                                        └── device_health_errors
```

### Monitoring

```bash
# Check pipeline status
just status

# View logs
just logs

# See what's in MinIO
just tree

# Check ingestion history
just ingestion-status 2025-01-03
```

### Web UIs

| Service | URL | Login |
|---------|-----|-------|
| Airflow (orchestration) | http://localhost:8080 | admin / admin |
| MinIO (storage) | http://localhost:9001 | minioadmin / minioadmin |
| Trino (SQL) | http://localhost:8081 | — |
| Spark (jobs) | http://localhost:8082 | — |

---

## 🔍 Querying the Data

### Connect to Trino

```bash
docker exec -it trino trino
```

### Example Queries

```sql
-- Count rounds by course
SELECT course_id, COUNT(DISTINCT round_id) as rounds
FROM iceberg.silver.fact_telemetry_event
GROUP BY course_id;

-- Average pace by hole
SELECT 
  hole_number,
  AVG(pace) as avg_pace,
  COUNT(*) as fixes
FROM iceberg.silver.fact_telemetry_event
WHERE course_id = 'indiancreek'
GROUP BY hole_number
ORDER BY hole_number;

-- Find slow rounds
SELECT *
FROM iceberg.gold.pace_summary_by_round
WHERE avg_pace > 500  -- More than 500 seconds behind goal
ORDER BY avg_pace DESC;
```

---

## 📁 File Locations

| What | Where |
|------|-------|
| Input files | `data/*.csv` or `data/*.json` |
| Bronze (raw) | MinIO: `tm-lakehouse-landing-zone/` |
| Silver (cleaned) | MinIO: `tm-lakehouse-source-store/warehouse/silver/` |
| Gold (analytics) | MinIO: `tm-lakehouse-serve/warehouse/gold/` |
| Quarantine | MinIO: `tm-lakehouse-quarantine/` |
| Run logs | MinIO: `tm-lakehouse-observability/` |

---

## 🔄 The Transformation Explained

### Input: 1 Round with Section Records (example: 46 sections)

```json
{
  "_id": "6724cb30657f3deb031720f3",
  "course": "indiancreek",
  "locations": [
    {"hole": 1, "holeSection": 2, "startTime": 300, "fixCoordinates": [-80.139, 25.874], "pace": 0},
    {"hole": 1, "holeSection": 3, "startTime": 769, "fixCoordinates": [-80.138, 25.874], "pace": -319},
    {"hole": 2, "holeSection": 1, "startTime": 980, "fixCoordinates": [-80.137, 25.874], "pace": -200},
    // ... 43 more section records
  ]
}
```

### Output: 46 Rows in Silver Table

| round_id | course_id | hole | section | timestamp | longitude | latitude | pace |
|----------|-----------|------|---------|-----------|-----------|----------|------|
| 6724cb30... | indiancreek | 1 | fairway | 2024-11-01 12:35:52 | -80.139 | 25.874 | 0 |
| 6724cb30... | indiancreek | 1 | green | 2024-11-01 12:43:41 | -80.138 | 25.874 | -319 |
| 6724cb30... | indiancreek | 2 | tee | 2024-11-01 12:47:12 | -80.137 | 25.874 | -200 |
| ... | ... | ... | ... | ... | ... | ... | ... |

This "long format" is much easier to query and aggregate!

**Pace interpretation:**
- `pace = 0` → On pace with goal time
- `pace = -319` → 319 seconds (5+ min) **ahead** of goal
- `pace = 500` → 500 seconds (8+ min) **behind** goal

---

## ❓ Common Questions

**Q: Is this raw GPS data?**
No! This is **processed round data** (round strings). TagMarshal's system has already:
- Collected raw GPS pings from devices
- Processed them into section-by-section records
- Calculated pace metrics
- Assigned hole/section information

We're building analytics on top of this pre-processed data.

**Q: What's the difference between CSV and JSON input?**
- CSV: Pre-flattened columns (`locations[0].hole`, `locations[1].hole`, etc.)
- JSON: Nested array structure (from MongoDB/API)
- Both work! The pipeline auto-detects the format.

**Q: What happens to bad data?**
- Invalid coordinates (lat/lon out of range) → Quarantine bucket
- Parse errors → Job fails with clear error message
- Missing fields → Filled with NULL

**Q: How do I add a new course?**
- Just drop the CSV/JSON file in `data/` folder
- Run `just bronze-upload-all` or `just pipeline-all`
- Course ID is extracted from filename (e.g., `erinhills.rounds.csv` → `erinhills`)

**Q: How do I re-process data?**
```bash
# Clear the registry for that date
just registry-clear 2025-01-03

# Re-run the pipeline
just pipeline-all 2025-01-03
```

---

## 📋 Project Context

This lakehouse is **Phase 1** of a larger data strategy:

| Phase | Focus | Status |
|-------|-------|--------|
| **Phase 1** | 5 pilot courses, local pipeline validation | 🔄 In Progress |
| **Phase 2** | AWS deployment, 20-50 course validation | Planned |
| **Phase 3-4** | Full 650-course rollout, dashboards | Planned |

The architecture supports **twin ingestion paths**:
- **Operational (now):** Processed round strings → Gold analytics
- **Innovation (future):** Raw telemetry → Bronze → ML/advanced analytics

