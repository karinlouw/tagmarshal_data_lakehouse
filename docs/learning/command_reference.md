# Command Reference

> Quick reference for all `just` commands in the TagMarshal Data Lakehouse.

---

## 🚀 Getting Started

| Command | Description |
|---------|-------------|
| `just up` | Start all Docker containers |
| `just down` | Stop all containers (keeps data) |
| `just restart` | Restart all containers |
| `just wait` | Wait for Airflow to be ready after startup |

**Example:**
```bash
just up          # Start the stack
just wait        # Wait for Airflow (if needed)
just ui          # Show all service URLs
```

---

## 📊 Pipeline Commands

### Full Pipeline
| Command | Description |
|---------|-------------|
| `just pipeline-all` | Run Bronze → Silver → Gold for all files |
| `just pipeline-all 2024-06-15` | Run full pipeline for specific date |

### Bronze Layer (Upload to MinIO)
| Command | Description |
|---------|-------------|
| `just bronze-upload <course> <file>` | Upload single file |
| `just bronze-upload-all` | Upload ALL files in `data/` folder |
| `just bronze-upload-all 2024-06-15` | Upload with specific ingest date |

**Examples:**
```bash
just bronze-upload indiancreek data/indiancreek.rounds.csv
just bronze-upload-all
just bronze-upload-all 2024-06-15
```

### Silver Layer (Spark ETL)
| Command | Description |
|---------|-------------|
| `just silver <course>` | Process ALL dates for a course |
| `just silver <course> <date>` | Process specific date for a course |
| `just silver-all` | Process ALL courses (today's date) |
| `just silver-all 2024-06-15` | Process ALL courses for specific date |

**Examples:**
```bash
just silver indiancreek              # All dates for indiancreek
just silver indiancreek 2024-06-15   # Specific date
just silver-all                      # All courses, today
just silver-all 2024-06-15           # All courses, specific date
```

### Gold Layer (dbt Analytics)
| Command | Description |
|---------|-------------|
| `just gold` | Run all dbt Gold models |

**Models created:**
- `gold.pace_summary_by_round`
- `gold.signal_quality_rounds`
- `gold.device_health_errors`

---

## 🔍 Query Commands (Trino)

| Command | Description |
|---------|-------------|
| `just trino` | Open interactive SQL shell |
| `just trino-query "<SQL>"` | Run a single query |
| `just trino-status` | Check if Trino is healthy |

**Examples:**
```bash
just trino                                                    # Interactive shell
just trino-query "SELECT count(*) FROM iceberg.silver.fact_telemetry_event"
just trino-query "SHOW TABLES IN iceberg.gold"
just trino-status                                             # Check health
```

**In the Trino shell:**
- Type `USE iceberg.silver;` to set default schema
- Type `quit` or press `Ctrl+D` to exit

---

## 📋 Status & Monitoring

| Command | Description |
|---------|-------------|
| `just status` | Check pipeline status (Airflow DAG runs) |
| `just health` | Show container health |
| `just ui` | Show all service URLs |
| `just logs` | Tail logs from all containers |

**Service URLs:**
- Airflow: http://localhost:8080 (admin/admin)
- MinIO: http://localhost:9001 (minioadmin/minioadmin)
- Trino: http://localhost:8081
- Spark: http://localhost:8082
- Superset: http://localhost:8088 (admin/admin)

---

## 📊 Superset (Dashboards)

| Command | Description |
|---------|-------------|
| `just superset` | Open Superset in browser |
| `just superset-status` | Check if Superset is ready |
| `just superset-trino-connection` | Show connection string for Trino |

**Setup:**
1. Run `just superset-trino-connection` to see the connection string
2. In Superset: Settings → Database Connections → + Database
3. Select Trino, paste: `trino://trino@trino:8080/iceberg`

**Available Schemas:**
- `silver` - Raw telemetry (fact_telemetry_event)
- `gold` - Analytics tables (pace_summary, data_quality, etc.)

---

## 📦 MinIO (Storage)

| Command | Description |
|---------|-------------|
| `just ls` | List all buckets |
| `just ls <bucket>` | List files in a bucket |
| `just ls <bucket>/<prefix>` | List files with prefix |
| `just view <path>` | View file contents (CSV/JSON/text) |
| `just view-all <path>` | View entire file (no line limit) |
| `just gold-obs` | View latest Gold observability summary |
| `just tree` | Show all bucket contents |
| `just tree landing` | Show specific bucket (`landing`, `source`, `serve`) |
| `just bucket-sizes` | Show size of each bucket |

**Examples:**
```bash
# List files
just ls                                                    # Show all buckets
just ls tm-lakehouse-landing-zone                          # List Bronze files
just ls tm-lakehouse-observability/gold                    # List Gold observability

# View files
just view tm-lakehouse-landing-zone/course_id=americanfalls/ingest_date=2026-01-04/americanfalls.rounds.csv
just view tm-lakehouse-observability/gold/run_id=20260104T135009Z/summary.json

# View Gold run summary
just gold-obs                                              # Latest Gold run summary
```

**Buckets:**
- `tm-lakehouse-landing-zone` - Bronze (raw files)
- `tm-lakehouse-source-store` - Silver (Iceberg tables)
- `tm-lakehouse-serve` - Gold (analytics tables)
- `tm-lakehouse-quarantine` - Invalid data
- `tm-lakehouse-observability` - Run logs

---

## 📝 Ingestion Registry

| Command | Description |
|---------|-------------|
| `just registry-init` | Initialize registry table (run once) |
| `just ingestion-status` | Show today's ingestion status |
| `just ingestion-status 2024-06-15` | Status for specific date |
| `just ingestion-summary` | Summary counts by status |
| `just ingestion-history <course>` | Full history for a course |
| `just ingestion-missing` | Courses not ingested today |
| `just registry-clear 2024-06-15` | Clear registry for a date |

**Examples:**
```bash
just ingestion-status                    # Today's status
just ingestion-history bradshawfarmgc    # History for Bradshaw Farm
just ingestion-missing silver            # Missing Silver ETL jobs
```

---

## 🔄 Backfill Commands (Bulk Processing)

| Command | Description |
|---------|-------------|
| `just backfill-status` | What's done, pending, failed |
| `just backfill-preview` | Dry run - see what would process |
| `just backfill-silver` | Run resumable Silver backfill |
| `just backfill-silver bradshawfarmgc` | Backfill specific course |
| `just backfill-retry` | Retry all failed jobs |
| `just backfill-reset` | Clear all tracking (start fresh) |

**Examples:**
```bash
just backfill-status                      # Check progress
just backfill-preview                     # See what needs processing
just backfill-silver                      # Process all pending
just backfill-silver bradshawfarmgc 5     # Process with batch size 5
just backfill-retry silver                # Retry failed Silver jobs
```

---

## 🔧 Maintenance

| Command | Description |
|---------|-------------|
| `just restart` | Restart all containers |
| `just restart-service airflow` | Restart specific service |
| `just rebuild` | Rebuild all containers |
| `just rebuild-service spark` | Rebuild specific service |
| `just nuke` | ⚠️ Delete ALL data and stop containers |
| `just reset-local` | ⚠️ Delete all Docker volumes |

**After `just nuke`:**
```bash
just up    # Start fresh with empty databases
```

---

## 📊 Dashboard Commands

The Streamlit dashboard provides a visual interface for data quality analysis.

| Command | Description |
|---------|-------------|
| `just dashboard-install` | Install Python dependencies for dashboard |
| `just dashboard` | Run dashboard (foreground, shows logs) |
| `just dashboard-bg` | Run dashboard in background |
| `just dashboard-stop` | Stop background dashboard |

**Prerequisites:**
- Trino must be running (`just trino-status`)
- Gold models must be built (`just gold`)

**Access:** http://localhost:8501

---

## 📁 Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────┐
│                     DAILY OPERATIONS                            │
├─────────────────────────────────────────────────────────────────┤
│  just up                    # Start stack                       │
│  just bronze-upload-all     # Upload new data                   │
│  just silver-all            # Process to Silver                 │
│  just gold                  # Build analytics                   │
│  just trino                 # Query results                     │
│  just dashboard             # Data quality dashboard            │
│  just down                  # Stop when done                    │
├─────────────────────────────────────────────────────────────────┤
│                     VIEW FILES (MinIO)                          │
├─────────────────────────────────────────────────────────────────┤
│  just ls                    # List all buckets                  │
│  just ls <bucket>           # List files in bucket              │
│  just view <path>           # View file (CSV/JSON/text)         │
│  just gold-obs              # Latest Gold run summary           │
├─────────────────────────────────────────────────────────────────┤
│                     TROUBLESHOOTING                             │
├─────────────────────────────────────────────────────────────────┤
│  just health                # Container status                  │
│  just status                # Pipeline status                   │
│  just trino-status          # Trino health                      │
│  just logs                  # View logs                         │
│  just restart               # Restart everything                │
├─────────────────────────────────────────────────────────────────┤
│                     BULK PROCESSING                             │
├─────────────────────────────────────────────────────────────────┤
│  just backfill-status       # Check progress                    │
│  just backfill-silver       # Resume bulk processing            │
│  just backfill-retry        # Retry failures                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Common Workflows

### First Time Setup
```bash
just up                  # Start Docker stack
just wait                # Wait for Airflow
just bronze-upload-all   # Upload sample data
just silver-all          # Process to Silver
just gold                # Build Gold tables
just trino               # Query results!
```

### Daily Data Refresh
```bash
just bronze-upload-all   # Upload today's data
just silver-all          # Process new data
just gold                # Refresh analytics
```

### Investigating a Course
```bash
just ingestion-history bradshawfarmgc    # Check history
just trino-query "SELECT * FROM iceberg.silver.fact_telemetry_event WHERE course_id='bradshawfarmgc' LIMIT 10"
```

### After System Crash
```bash
just health              # Check what's running
just restart             # Restart everything
just backfill-status     # Check what's pending
just backfill-silver     # Resume processing
```

### Starting Fresh
```bash
just nuke                # Delete everything
just up                  # Start fresh
just bronze-upload-all   # Re-upload data
just silver-all          # Re-process
just gold                # Rebuild analytics
```

### Client Demo (Data Quality Dashboard)
```bash
just trino-status        # Ensure Trino is ready
just dashboard-install   # First time: install dependencies
just dashboard           # Launch dashboard at http://localhost:8501
```

---

*Last updated: January 2026*

