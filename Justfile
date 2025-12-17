set dotenv-load := false

# Use `just --list` to see commands.

default:
  @just --list

up:
  docker compose --env-file config/local.env up -d

down:
  docker compose --env-file config/local.env down

# Restart all containers (quick restart without rebuild)
restart:
  @echo ""
  @echo "🔄 RESTARTING CONTAINERS"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  docker compose --env-file config/local.env restart
  @echo ""
  @echo "✅ Containers restarted"
  @echo ""

# Restart a specific service
restart-service service:
  @echo ""
  @echo "🔄 RESTARTING: {{service}}"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  docker compose --env-file config/local.env restart {{service}}
  @echo "✅ {{service}} restarted"
  @echo ""

# Rebuild all containers (pull latest images, recreate)
rebuild:
  @echo ""
  @echo "🔨 REBUILDING ALL CONTAINERS"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  docker compose --env-file config/local.env down
  docker compose --env-file config/local.env build --pull
  docker compose --env-file config/local.env up -d
  @echo ""
  @echo "✅ All containers rebuilt and started"
  @just ui

# Rebuild a specific service
rebuild-service service:
  @echo ""
  @echo "🔨 REBUILDING: {{service}}"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  docker compose --env-file config/local.env stop {{service}}
  docker compose --env-file config/local.env rm -f {{service}}
  docker compose --env-file config/local.env up -d {{service}}
  @echo "✅ {{service}} rebuilt and started"
  @echo ""

# Nuclear option: wipe everything and start fresh (deletes all data!)
nuke:
  @echo ""
  @echo "☢️  NUKING EVERYTHING (this deletes all data!)"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  @echo "  Stopping containers..."
  docker compose --env-file config/local.env down -v --remove-orphans
  @echo "  Pruning Docker resources..."
  docker system prune -f
  @echo "  Starting fresh..."
  docker compose --env-file config/local.env up -d
  @echo ""
  @echo "✅ Fresh start complete"
  @just ui

# Show container health status
health:
  @echo ""
  @echo "🏥 CONTAINER HEALTH"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  @docker compose --env-file config/local.env ps
  @echo ""

logs:
  docker compose --env-file config/local.env logs -f --tail=200

ui:
  @echo ""
  @echo "🌐 SERVICE URLS"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  @echo "  Airflow   http://localhost:8080"
  @echo "  Trino     http://localhost:8081"
  @echo "  Spark     http://localhost:8082"
  @echo "  MinIO     http://localhost:9001"
  @echo ""

# Show MinIO bucket structure (tree view)
tree bucket="":
  #!/usr/bin/env bash
  set -euo pipefail
  
  # Ensure alias exists
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null
  
  echo ""
  echo "🗂️  MINIO BUCKET STRUCTURE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  if [ -n "{{bucket}}" ]; then
    echo "  Bucket: {{bucket}}"
    echo ""
    docker exec minio mc ls myminio/{{bucket}}/ --recursive 2>/dev/null | head -50
  else
    echo ""
    echo "  📦 tm-lakehouse-landing-zone (Raw CSV)"
    docker exec minio mc ls myminio/tm-lakehouse-landing-zone/ 2>/dev/null | head -10
    echo ""
    echo "  📊 tm-lakehouse-source-store (Silver Iceberg)"
    docker exec minio mc ls myminio/tm-lakehouse-source-store/warehouse/silver/ 2>/dev/null | head -5 || echo "    (empty)"
    echo ""
    echo "  🏆 tm-lakehouse-serve (Gold Iceberg)"
    docker exec minio mc ls myminio/tm-lakehouse-serve/warehouse/gold/ 2>/dev/null | head -5 || echo "    (empty)"
    echo ""
    echo "  🚫 tm-lakehouse-quarantine (Invalid data)"
    docker exec minio mc ls myminio/tm-lakehouse-quarantine/ 2>/dev/null | head -5 || echo "    (empty)"
    echo ""
    echo "  📋 tm-lakehouse-observability (Run logs)"
    docker exec minio mc ls myminio/tm-lakehouse-observability/ 2>/dev/null | head -5
  fi
  
  echo ""
  echo "🔗 Browse: http://localhost:9001"
  echo ""

# Show bucket size summary
bucket-sizes:
  #!/usr/bin/env bash
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null
  
  echo ""
  echo "📊 BUCKET SIZES"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  for bucket in tm-lakehouse-landing-zone tm-lakehouse-source-store tm-lakehouse-serve tm-lakehouse-quarantine tm-lakehouse-observability; do
    size=$(docker exec minio mc du myminio/$bucket/ 2>/dev/null | tail -1 | awk '{print $1}')
    printf "  %-35s %s\n" "$bucket" "${size:-0B}"
  done
  echo ""

# Upload a single file to Landing Zone
bronze-upload course_id local_path ingest_date="":
  #!/usr/bin/env bash
  set -euo pipefail
  INGEST_DATE="{{ingest_date}}"
  [ -z "$INGEST_DATE" ] && INGEST_DATE=$(date +%Y-%m-%d)
  
  echo ""
  echo "📦 BRONZE UPLOAD"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Course: {{course_id}}"
  echo "  File:   {{local_path}}"
  echo "  Date:   $INGEST_DATE"
  echo ""
  echo "  ⏳ Triggering..."
  docker exec airflow airflow dags trigger bronze_ingest \
    --conf '{"course_id":"{{course_id}}","local_path":"{{local_path}}","ingest_date":"'"$INGEST_DATE"'"}' \
    2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
  echo ""
  echo "✅ TRIGGERED"
  echo "  📍 Target: s3://tm-lakehouse-landing-zone/course_id={{course_id}}/ingest_date=$INGEST_DATE/"
  echo "  🔗 Monitor: http://localhost:8080/dags/bronze_ingest"
  echo ""

# Upload ALL CSV files in data/ folder to Landing Zone
bronze-upload-all ingest_date="":
  #!/usr/bin/env bash
  set -euo pipefail
  INGEST_DATE="${1:-$(date +%Y-%m-%d)}"
  
  echo ""
  echo "📦 BRONZE UPLOAD"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date: $INGEST_DATE"
  echo ""
  
  # Collect file info for summary
  declare -a courses=()
  declare -a filenames=()
  declare -a rowcounts=()
  declare -a paths=()
  
  for file in data/*.csv; do
    filename=$(basename "$file")
    course_id=$(echo "$filename" | sed -E 's/\.rounds.*//; s/\..*//')
    container_path="/opt/tagmarshal/input/$filename"
    rows=$(($(wc -l < "$file") - 1))  # subtract header
    s3_path="s3://tm-lakehouse-landing-zone/course_id=$course_id/ingest_date=$INGEST_DATE/$filename"
    
    # Store for summary
    courses+=("$course_id")
    filenames+=("$filename")
    rowcounts+=("$rows")
    paths+=("$s3_path")
    
    echo "  ⏳ $course_id"
    docker exec airflow airflow dags trigger bronze_ingest \
      --conf "{\"course_id\":\"$course_id\",\"local_path\":\"$container_path\",\"ingest_date\":\"$INGEST_DATE\"}" \
      2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
    sleep 1
  done
  
  # Summary table
  echo ""
  echo "✅ TRIGGERED ${#courses[@]} UPLOADS"
  echo ""
  echo "┌────────────────────┬────────────────────────────────────────┬──────────┐"
  echo "│ Course             │ Filename                               │ Rows     │"
  echo "├────────────────────┼────────────────────────────────────────┼──────────┤"
  for i in "${!courses[@]}"; do
    printf "│ %-18s │ %-38s │ %8s │\n" "${courses[$i]}" "${filenames[$i]:0:38}" "${rowcounts[$i]}"
  done
  echo "└────────────────────┴────────────────────────────────────────┴──────────┘"
  echo ""
  echo "📍 Storage Locations:"
  echo "   S3 path: s3://tm-lakehouse-landing-zone/course_id=<course>/ingest_date=$INGEST_DATE/"
  echo "   MinIO:   http://localhost:9001/browser/tm-lakehouse-landing-zone"
  echo ""
  echo "🔗 Monitor: http://localhost:8080/dags/bronze_ingest"
  echo ""

# Process a single course through Silver ETL
silver course_id ingest_date bronze_prefix:
  #!/usr/bin/env bash
  set -euo pipefail
  
  echo ""
  echo "⚙️  SILVER ETL"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Course: {{course_id}}"
  echo "  Date:   {{ingest_date}}"
  echo "  Source: s3://tm-lakehouse-landing-zone/{{bronze_prefix}}"
  echo ""
  echo "  ⏳ Triggering..."
  docker exec airflow airflow dags trigger silver_etl \
    --conf '{"course_id":"{{course_id}}","ingest_date":"{{ingest_date}}","bronze_prefix":"{{bronze_prefix}}"}' \
    2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
  echo ""
  echo "✅ TRIGGERED"
  echo "  📍 Target: silver.fact_telemetry_event (Iceberg)"
  echo "  🔗 Monitor: http://localhost:8080/dags/silver_etl"
  echo ""

# Process ALL courses through Silver ETL (after bronze-upload-all)
silver-all ingest_date="":
  #!/usr/bin/env bash
  set -euo pipefail
  INGEST_DATE="${1:-$(date +%Y-%m-%d)}"
  
  echo ""
  echo "⚙️  SILVER ETL"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date: $INGEST_DATE"
  echo ""
  
  # Collect course info for summary
  declare -a courses=()
  declare -a landing_paths=()
  
  for file in data/*.csv; do
    filename=$(basename "$file")
    course_id=$(echo "$filename" | sed -E 's/\.rounds.*//; s/\..*//')
    bronze_prefix="course_id=$course_id/ingest_date=$INGEST_DATE/"
    landing_path="s3://tm-lakehouse-landing-zone/$bronze_prefix"
    
    # Store for summary
    courses+=("$course_id")
    landing_paths+=("$landing_path")
    
    echo "  ⏳ $course_id"
    docker exec airflow airflow dags trigger silver_etl \
      --conf "{\"course_id\":\"$course_id\",\"ingest_date\":\"$INGEST_DATE\",\"bronze_prefix\":\"$bronze_prefix\"}" \
      2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
    sleep 1
  done
  
  # Summary table
  echo ""
  echo "✅ TRIGGERED ${#courses[@]} ETL JOBS"
  echo ""
  echo "┌────────────────────┬──────────────────────────────────────────────────────┐"
  echo "│ Course             │ Landing Zone Source                                  │"
  echo "├────────────────────┼──────────────────────────────────────────────────────┤"
  for i in "${!courses[@]}"; do
    printf "│ %-18s │ %-52s │\n" "${courses[$i]}" "${landing_paths[$i]:0:52}"
  done
  echo "└────────────────────┴──────────────────────────────────────────────────────┘"
  echo ""
  echo "📍 Storage Locations:"
  echo "   Table:  silver.fact_telemetry_event (Iceberg)"
  echo "   MinIO:  http://localhost:9001/browser/tm-lakehouse-source-store/warehouse/silver/"
  echo ""
  echo "🔗 Monitor: http://localhost:8080/dags/silver_etl"
  echo ""

# Run Gold dbt models
gold:
  #!/usr/bin/env bash
  set -euo pipefail
  
  echo ""
  echo "🏆 GOLD DBT MODELS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  echo "  ⏳ Triggering..."
  docker exec airflow airflow dags trigger gold_dbt \
    2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
  echo ""
  echo "✅ TRIGGERED"
  echo ""
  echo "  Models:"
  echo "    • gold.pace_summary_by_round"
  echo "    • gold.signal_quality_rounds"
  echo "    • gold.device_health_errors"
  echo ""
  echo "📍 Storage Locations:"
  echo "   MinIO: http://localhost:9001/browser/tm-lakehouse-serve/warehouse/gold/"
  echo ""
  echo "🔗 Monitor: http://localhost:8080/dags/gold_dbt"
  echo ""

# Full pipeline: Bronze → Silver → Gold for all files
pipeline-all ingest_date="":
  #!/usr/bin/env bash
  set -euo pipefail
  INGEST_DATE="${1:-$(date +%Y-%m-%d)}"
  
  echo ""
  echo "🚀 FULL PIPELINE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date:  $INGEST_DATE"
  echo "  Steps: Bronze → Silver → Gold"
  echo ""
  
  echo "────────────────────────────────────────"
  echo "📦 STEP 1/3: Bronze Upload"
  echo "────────────────────────────────────────"
  just bronze-upload-all "$INGEST_DATE"
  
  echo "  ⏳ Waiting 30s for Bronze jobs..."
  sleep 30
  
  echo "────────────────────────────────────────"
  echo "⚙️  STEP 2/3: Silver ETL"
  echo "────────────────────────────────────────"
  just silver-all "$INGEST_DATE"
  
  echo "  ⏳ Waiting 60s for Silver jobs..."
  sleep 60
  
  echo "────────────────────────────────────────"
  echo "🏆 STEP 3/3: Gold Models"
  echo "────────────────────────────────────────"
  just gold
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "✅ PIPELINE COMPLETE"
  echo ""
  echo "  🔗 Monitor: http://localhost:8080"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""

# Check pipeline status
status:
  #!/usr/bin/env bash
  echo ""
  echo "📊 PIPELINE STATUS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  echo "📦 Bronze Ingest:"
  docker exec airflow airflow dags list-runs -d bronze_ingest -o table 2>&1 | grep -v "INFO\|WARNING\|FutureWarning\|__init__\|auth.backend\|configuration.py\|deprecated" | head -8 || echo "  No runs yet"
  echo ""
  echo "⚙️  Silver ETL:"
  docker exec airflow airflow dags list-runs -d silver_etl -o table 2>&1 | grep -v "INFO\|WARNING\|FutureWarning\|__init__\|auth.backend\|configuration.py\|deprecated" | head -8 || echo "  No runs yet"
  echo ""
  echo "🏆 Gold dbt:"
  docker exec airflow airflow dags list-runs -d gold_dbt -o table 2>&1 | grep -v "INFO\|WARNING\|FutureWarning\|__init__\|auth.backend\|configuration.py\|deprecated" | head -8 || echo "  No runs yet"
  echo ""
  echo "  🔗 Full details: http://localhost:8080"
  echo ""

dq course_id ingest_date bronze_prefix:
  @echo "DQ is part of silver_etl DAG (Spark → dbt test → upload artifacts)."
  @just silver {{course_id}} {{ingest_date}} {{bronze_prefix}}

# Initialize the ingestion registry table (run once after first 'just up')
registry-init:
  #!/usr/bin/env bash
  echo ""
  echo "🗄️  REGISTRY INIT"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Creating ingestion_log table..."
  echo ""
  docker exec -i airflow-postgres psql -U airflow -d airflow < db/migrations/001_create_ingestion_log.sql 2>&1 | grep -v "NOTICE\|already exists" || true
  echo ""
  echo "✅ Registry initialized"
  echo ""

# Show ingestion status for a date (default: today)
ingestion-status ingest_date="":
  #!/usr/bin/env bash
  INGEST_DATE="${1:-$(date +%Y-%m-%d)}"
  echo ""
  echo "📋 INGESTION STATUS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date: $INGEST_DATE"
  echo ""
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    SELECT 
      course_id,
      layer,
      status,
      rows_processed as rows,
      ROUND(duration_seconds::numeric, 1) as secs,
      TO_CHAR(completed_at, 'HH24:MI:SS') as completed
    FROM ingestion_log
    WHERE ingest_date = '$INGEST_DATE'
    ORDER BY completed_at DESC NULLS LAST;
  " 2>&1 | grep -v "INFO\|WARNING\|FutureWarning" || echo "  No ingestions found for $INGEST_DATE"
  echo ""

# Show summary counts by status
ingestion-summary ingest_date="":
  #!/usr/bin/env bash
  INGEST_DATE="${1:-$(date +%Y-%m-%d)}"
  echo ""
  echo "📊 INGESTION SUMMARY"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date: $INGEST_DATE"
  echo ""
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    SELECT 
      layer,
      status,
      COUNT(*) as count,
      SUM(rows_processed) as total_rows
    FROM ingestion_log
    WHERE ingest_date = '$INGEST_DATE'
    GROUP BY layer, status
    ORDER BY layer, status;
  " 2>&1 | grep -v "INFO\|WARNING\|FutureWarning" || echo "  No ingestions found for $INGEST_DATE"
  echo ""

# Show courses that haven't been ingested today but were ingested before
ingestion-missing layer="bronze":
  #!/usr/bin/env bash
  echo ""
  echo "⚠️  MISSING INGESTIONS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Layer: {{layer}}"
  echo "  (Courses ingested before but not today)"
  echo ""
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    SELECT DISTINCT 
      course_id,
      MAX(ingest_date) as last_ingested
    FROM ingestion_log
    WHERE layer = '{{layer}}' 
      AND status = 'success' 
      AND ingest_date < CURRENT_DATE
      AND course_id NOT IN (
          SELECT course_id FROM ingestion_log 
          WHERE layer = '{{layer}}' AND status = 'success' AND ingest_date = CURRENT_DATE
      )
    GROUP BY course_id
    ORDER BY last_ingested DESC;
  " 2>&1 | grep -v "INFO\|WARNING\|FutureWarning" || echo "  No missing ingestions"
  echo ""

# Show complete ingestion history for a course
ingestion-history course_id:
  #!/usr/bin/env bash
  echo ""
  echo "📜 INGESTION HISTORY: {{course_id}}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    SELECT 
      ingest_date,
      layer,
      status,
      rows_processed as rows,
      ROUND(duration_seconds::numeric, 1) as secs,
      TO_CHAR(completed_at, 'YYYY-MM-DD HH24:MI') as completed
    FROM ingestion_log
    WHERE course_id = '{{course_id}}'
    ORDER BY ingest_date DESC, layer;
  " 2>&1 | grep -v "INFO\|WARNING\|FutureWarning" || echo "  No history found for {{course_id}}"
  echo ""

# Clear registry entries for a specific date (use when MinIO is out of sync)
registry-clear ingest_date="":
  #!/usr/bin/env bash
  INGEST_DATE="${1:-$(date +%Y-%m-%d)}"
  echo ""
  echo "🗑️  CLEARING REGISTRY"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date: $INGEST_DATE"
  echo ""
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    DELETE FROM ingestion_log WHERE ingest_date = '$INGEST_DATE';
  " 2>&1 | grep -v "INFO\|WARNING"
  echo ""
  echo "✅ Registry cleared for $INGEST_DATE"
  echo "  You can now re-run: just pipeline-all $INGEST_DATE"
  echo ""

reset-local:
  @echo ""
  @echo "⚠️  RESET LOCAL DATA"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  @echo "  This will DELETE all local data!"
  @echo ""
  docker compose --env-file config/local.env down -v
  @echo ""
  @echo "✅ Local volumes wiped"
  @echo "  Run 'just up' to start fresh"
  @echo ""
