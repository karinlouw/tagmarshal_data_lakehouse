set dotenv-load := false

# Use `just --list` to see commands.

default:
  @just --list

up:
  #!/usr/bin/env bash
  set -euo pipefail
  
  echo ""
  echo "🚀 STARTING LAKEHOUSE STACK"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  docker compose --env-file config/local.env up -d
  
  echo ""
  echo "⏳ Waiting for Airflow to initialize (this takes ~60-90 seconds)..."
  echo "   (Installing dependencies, initializing database, creating admin user)"
  echo ""
  
  # Wait for Airflow webserver to be ready (max 120 seconds)
  for i in {1..24}; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null | grep -q "200"; then
      echo "✅ Airflow is ready!"
      break
    fi
    if [ $i -eq 24 ]; then
      echo "⚠️  Airflow is still starting. Run 'just wait' to check again, or check logs with 'docker logs airflow'"
      break
    fi
    printf "   [%02d/24] Waiting... " $i
    # Show a hint of what's happening
    STATUS=$(docker logs airflow --tail 1 2>/dev/null | head -c 60 || echo "starting...")
    echo "$STATUS"
    sleep 5
  done
  
  echo ""
  just ui

# Wait for Airflow to be ready (use after 'just up' if Airflow wasn't ready)
wait:
  #!/usr/bin/env bash
  echo ""
  echo "⏳ WAITING FOR AIRFLOW"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  for i in {1..30}; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null | grep -q "200"; then
      echo ""
      echo "✅ Airflow is ready! http://localhost:8080"
      echo "   Login: admin / admin"
      exit 0
    fi
    printf "   [%02d/30] " $i
    docker logs airflow --tail 1 2>/dev/null | head -c 70 || echo "starting..."
    sleep 5
  done
  
  echo ""
  echo "❌ Airflow didn't start in time. Check logs:"
  echo "   docker logs airflow --tail 50"

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
  @echo ""
  @echo "✅ Nuke complete - all data deleted"
  @echo ""
  @echo "  To start fresh, run:"
  @echo "    just up"
  @echo ""

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

# Open interactive Trino SQL shell
trino:
  @echo ""
  @echo "🔷 TRINO SQL SHELL"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  @echo "  Exit: Ctrl+D (or type 'quit' on empty line)"
  @echo "  Try:  USE iceberg.silver;"
  @echo ""
  @docker exec -it trino trino

# Run a Trino SQL query
trino-query query:
  @docker exec trino trino --execute "{{query}}" 2>&1 | grep -v "jline\|WARNING"

# Check Trino health and status
trino-status:
  #!/usr/bin/env bash
  echo ""
  echo "🔷 TRINO STATUS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if docker ps | grep -q "trino.*healthy"; then
    echo "  ✅ Trino is healthy and ready"
  elif docker ps | grep -q "trino.*starting"; then
    echo "  ⏳ Trino is starting up (wait ~3 min)"
  elif docker ps | grep -q "trino"; then
    echo "  ⚠️  Trino is running but not healthy yet"
  else
    echo "  ❌ Trino is not running"
  fi
  echo ""
  INFO=$(curl -s http://localhost:8081/v1/info 2>/dev/null)
  if [ -n "$INFO" ]; then
    echo "  Info:"
    echo "$INFO" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'    Version: {d[\"nodeVersion\"][\"version\"]}'); print(f'    Starting: {d[\"starting\"]}'); print(f'    Uptime: {d[\"uptime\"]}')"
  fi
  echo ""

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
  
  # Convert local path to container path
  # data/file.csv → /opt/tagmarshal/input/file.csv
  FILENAME=$(basename "{{local_path}}")
  CONTAINER_PATH="/opt/tagmarshal/input/$FILENAME"
  
  echo ""
  echo "📦 BRONZE UPLOAD"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Course: {{course_id}}"
  echo "  File:   {{local_path}}"
  echo "  Date:   $INGEST_DATE"
  echo ""
  echo "  ⏳ Triggering..."
  docker exec airflow airflow dags trigger bronze_ingest \
    --conf '{"course_id":"{{course_id}}","local_path":"'"$CONTAINER_PATH"'","ingest_date":"'"$INGEST_DATE"'"}' \
    2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
  echo ""
  echo "✅ TRIGGERED"
  echo "  📍 Target: s3://tm-lakehouse-landing-zone/course_id={{course_id}}/ingest_date=$INGEST_DATE/"
  echo "  🔗 Monitor: http://localhost:8080/dags/bronze_ingest"
  echo ""

# Upload ALL CSV and JSON files in data/ folder to Landing Zone
bronze-upload-all ingest_date="":
  #!/usr/bin/env bash
  set -euo pipefail
  INGEST_DATE="${1:-$(date +%Y-%m-%d)}"
  
  echo ""
  echo "📦 BRONZE UPLOAD (ALL FILES)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date: $INGEST_DATE"
  echo ""
  
  # Wait for Airflow to be ready
  echo "  ⏳ Checking Airflow..."
  for i in {1..12}; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null | grep -q "200"; then
      echo "  ✅ Airflow is ready"
      break
    fi
    if [ $i -eq 12 ]; then
      echo "  ❌ Airflow not ready. Run 'just wait' first."
      exit 1
    fi
    sleep 5
  done
  echo ""
  
  # Collect file info for summary
  declare -a courses=()
  declare -a filenames=()
  declare -a rowcounts=()
  declare -a paths=()
  
  # Process CSV files
  for file in data/*.csv; do
    [ -f "$file" ] || continue
    filename=$(basename "$file")
    course_id=$(echo "$filename" | sed -E 's/\.rounds.*//; s/\..*//')
    container_path="/opt/tagmarshal/input/$filename"
    rows=$(($(wc -l < "$file") - 1))  # subtract header
    s3_path="s3://tm-lakehouse-landing-zone/course_id=$course_id/ingest_date=$INGEST_DATE/$filename"
    
    courses+=("$course_id")
    filenames+=("$filename")
    rowcounts+=("$rows")
    paths+=("$s3_path")
    
    echo "  ⏳ $course_id (CSV)"
    docker exec airflow airflow dags trigger bronze_ingest \
      --conf "{\"course_id\":\"$course_id\",\"local_path\":\"$container_path\",\"ingest_date\":\"$INGEST_DATE\"}" \
      2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
    sleep 1
  done
  
  # Process JSON files
  for file in data/*.json; do
    [ -f "$file" ] || continue
    filename=$(basename "$file")
    course_id=$(echo "$filename" | sed -E 's/\.rounds.*//; s/\..*//')
    container_path="/opt/tagmarshal/input/$filename"
    # Count JSON array elements (rough estimate)
    rows=$(grep -c '"_id"' "$file" 2>/dev/null || echo "?")
    s3_path="s3://tm-lakehouse-landing-zone/course_id=$course_id/ingest_date=$INGEST_DATE/$filename"
    
    courses+=("$course_id")
    filenames+=("$filename")
    rowcounts+=("$rows")
    paths+=("$s3_path")
    
    echo "  ⏳ $course_id (JSON)"
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
silver course_id ingest_date="":
  #!/usr/bin/env bash
  set -euo pipefail
  
  COURSE_ID="{{course_id}}"
  
  echo ""
  echo "⚙️  SILVER ETL"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Course: $COURSE_ID"
  
  if [ -n "{{ingest_date}}" ]; then
    # Specific date provided - process only that date
    INGEST_DATE="{{ingest_date}}"
    BRONZE_PREFIX="course_id=$COURSE_ID/ingest_date=$INGEST_DATE/"
    echo "  Date:   $INGEST_DATE"
    echo "  Source: s3://tm-lakehouse-landing-zone/$BRONZE_PREFIX"
    echo ""
    echo "  ⏳ Triggering..."
    docker exec airflow airflow dags trigger silver_etl \
      --conf "{\"course_id\":\"$COURSE_ID\",\"ingest_date\":\"$INGEST_DATE\",\"bronze_prefix\":\"$BRONZE_PREFIX\"}" \
      2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
    echo ""
    echo "✅ TRIGGERED"
  else
    # No date - process ALL dates for this course
    echo "  Date:   ALL (scanning MinIO for available dates)"
    echo ""
    
    # Get all ingest_date folders for this course from MinIO
    PYCMD='import boto3; s3 = boto3.client("s3", endpoint_url="http://minio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin"); result = s3.list_objects_v2(Bucket="tm-lakehouse-landing-zone", Prefix="course_id='"$COURSE_ID"'/ingest_date=", Delimiter="/"); [print(p["Prefix"].split("ingest_date=")[1].rstrip("/")) for p in result.get("CommonPrefixes", [])]'
    DATES=$(docker exec airflow python3 -c "$PYCMD" 2>/dev/null)
    
    if [ -z "$DATES" ]; then
      echo "  ❌ No data found for course '$COURSE_ID' in MinIO"
      exit 1
    fi
    
    COUNT=0
    for INGEST_DATE in $DATES; do
      BRONZE_PREFIX="course_id=$COURSE_ID/ingest_date=$INGEST_DATE/"
      echo "  ⏳ $INGEST_DATE"
      docker exec airflow airflow dags trigger silver_etl \
        --conf "{\"course_id\":\"$COURSE_ID\",\"ingest_date\":\"$INGEST_DATE\",\"bronze_prefix\":\"$BRONZE_PREFIX\"}" \
        2>&1 | grep -v "INFO\|WARNING\|__init__\|auth.backend" > /dev/null || true
      COUNT=$((COUNT + 1))
      sleep 0.5
    done
    echo ""
    echo "✅ TRIGGERED $COUNT ETL job(s)"
  fi
  
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

# ============================================================
# BACKFILL COMMANDS (for massive data ingestion)
# ============================================================

# Show backfill status - what's done, what's pending, what failed
backfill-status:
  #!/usr/bin/env bash
  echo ""
  echo "📊 BACKFILL STATUS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  echo "By Layer and Status:"
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    SELECT layer, status, count(*) as jobs
    FROM ingestion_log 
    GROUP BY layer, status 
    ORDER BY layer, status
  " 2>&1 | grep -v "rows)"
  echo ""
  echo "Recent Failures:"
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    SELECT course_id, ingest_date, layer, 
           LEFT(error_message, 50) as error
    FROM ingestion_log 
    WHERE status = 'failed' 
    ORDER BY completed_at DESC 
    LIMIT 5
  " 2>&1 | grep -v "rows)"
  echo ""

# Preview what would be processed (dry run)
backfill-preview layer="silver":
  #!/usr/bin/env bash
  echo ""
  echo "🔍 BACKFILL PREVIEW: {{layer}}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  python3 scripts/backfill.py {{layer}} --dry-run
  echo ""

# Run Silver backfill (resumable)
backfill-silver course_id="" batch_size="3":
  #!/usr/bin/env bash
  echo ""
  echo "🔄 SILVER BACKFILL"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  COURSE_ARG=""
  if [ -n "{{course_id}}" ]; then
    COURSE_ARG="--course {{course_id}}"
  fi
  
  python3 scripts/backfill.py silver $COURSE_ARG --batch-size {{batch_size}}
  echo ""

# Retry all failed jobs
backfill-retry layer="":
  #!/usr/bin/env bash
  echo ""
  echo "🔄 RETRY FAILED JOBS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  LAYER_ARG=""
  if [ -n "{{layer}}" ]; then
    LAYER_ARG="--layer {{layer}}"
  fi
  
  # Reset failed to pending
  docker exec airflow-postgres psql -U airflow -d airflow -c "
    UPDATE ingestion_log 
    SET status = 'pending', retry_count = retry_count + 1
    WHERE status = 'failed' $([ -n '{{layer}}' ] && echo \"AND layer = '{{layer}}'\")
  " 2>&1 | grep -v "UPDATE"
  
  echo "✅ Failed jobs reset to pending"
  echo "   Run 'just backfill-silver' to process them"
  echo ""

# Clear all backfill tracking (start fresh)
backfill-reset:
  #!/usr/bin/env bash
  echo ""
  echo "⚠️  RESET BACKFILL TRACKING"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  This clears all ingestion_log entries!"
  echo ""
  read -p "  Are you sure? (y/N) " confirm
  if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
    docker exec airflow-postgres psql -U airflow -d airflow -c "TRUNCATE ingestion_log"
    echo "✅ Backfill tracking cleared"
  else
    echo "Cancelled"
  fi
  echo ""
