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
  echo "🗂️  Initializing Iceberg schemas..."
  # Wait for Trino to be ready
  for i in {1..12}; do
    if curl -s http://localhost:8081/v1/info 2>/dev/null | grep -q "starting.*false"; then
      # Ensure silver schema exists
      docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3://tm-lakehouse-source-store/warehouse/silver')" 2>&1 | grep -v jline > /dev/null || true
      
      # Ensure gold schema has correct location (tm-lakehouse-serve)
      GOLD_LOCATION=$(docker exec trino trino --execute "SHOW CREATE SCHEMA iceberg.gold" 2>&1 | grep -o "s3://[^']*" || echo "")
      if [ "$GOLD_LOCATION" != "s3://tm-lakehouse-serve/warehouse/gold" ]; then
        docker exec trino trino --execute "DROP SCHEMA IF EXISTS iceberg.gold CASCADE" 2>&1 | grep -v jline > /dev/null || true
        docker exec trino trino --execute "CREATE SCHEMA iceberg.gold WITH (location = 's3://tm-lakehouse-serve/warehouse/gold')" 2>&1 | grep -v jline > /dev/null
      fi
      echo "✅ Schemas initialized (Silver → source-store, Gold → serve)"
      break
    fi
    if [ $i -eq 12 ]; then
      echo "⚠️  Trino not ready yet - schemas will be initialized when Trino starts"
      break
    fi
    sleep 5
  done
  
  echo ""
  just ui

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "💡 Run 'just verify' to check all dependencies are ready"
  echo "💡 Run 'just init-schemas' to fix schema locations if needed"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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

# Verify all services and dependencies are ready (run before bronze/silver)
verify:
  #!/usr/bin/env bash
  set -euo pipefail
  
  echo ""
  echo "🔍 VERIFYING LAKEHOUSE STACK"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  
  ERRORS=0
  
  # 1. Check containers are running
  echo "📦 Checking containers..."
  for container in spark spark-worker minio airflow trino iceberg-rest; do
    if docker ps --filter "name=^${container}$" --filter "status=running" -q | grep -q .; then
      printf "   ✅ %s\n" "$container"
    else
      printf "   ❌ %s (not running)\n" "$container"
      ERRORS=$((ERRORS + 1))
    fi
  done
  echo ""
  
  # 2. Check Spark has pre-baked JARs
  echo "📚 Checking Spark dependencies (pre-baked JARs)..."
  JARS=$(docker exec spark ls /opt/spark/extra-jars/ 2>/dev/null | wc -l || echo "0")
  if [ "$JARS" -ge 6 ]; then
    echo "   ✅ Found $JARS pre-baked JARs in /opt/spark/extra-jars/"
    docker exec spark ls -lh /opt/spark/extra-jars/ 2>/dev/null | tail -6 | while read line; do
      echo "      $line"
    done
  else
    echo "   ❌ Missing pre-baked JARs! Run: just rebuild"
    echo "      Expected 6+ JARs in /opt/spark/extra-jars/"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""
  
  # 3. Check MinIO is accessible and buckets exist
  echo "🪣 Checking MinIO buckets..."
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1 || true
  for bucket in tm-lakehouse-landing-zone tm-lakehouse-source-store tm-lakehouse-serve; do
    if docker exec minio mc ls myminio/$bucket/ > /dev/null 2>&1; then
      printf "   ✅ %s\n" "$bucket"
    else
      printf "   ❌ %s (not found)\n" "$bucket"
      ERRORS=$((ERRORS + 1))
    fi
  done
  echo ""
  
  # 4. Check Airflow is healthy
  echo "🌬️ Checking Airflow..."
  if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null | grep -q "200"; then
    echo "   ✅ Airflow API is healthy"
    # Check DAGs are loaded
    DAGS=$(docker exec airflow airflow dags list 2>&1 | grep -c "_" || echo "0")
    echo "   ✅ $DAGS DAGs loaded"
  else
    echo "   ❌ Airflow not ready (run: just wait)"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""
  
  # 5. Check Trino is healthy and initialize schemas
  echo "🔷 Checking Trino..."
  if curl -s http://localhost:8081/v1/info 2>/dev/null | grep -q "starting.*false"; then
    echo "   ✅ Trino is ready"
    # Check Iceberg catalog
    if docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg" 2>&1 | grep -q "silver\|default"; then
      echo "   ✅ Iceberg catalog connected"
    else
      echo "   ⚠️  Iceberg catalog may need initialization"
    fi
    # Ensure gold schema has correct location (tm-lakehouse-serve, not source-store)
    GOLD_LOCATION=$(docker exec trino trino --execute "SHOW CREATE SCHEMA iceberg.gold" 2>&1 | grep -o "s3://[^']*" || echo "")
    if [ "$GOLD_LOCATION" = "s3://tm-lakehouse-serve/warehouse/gold" ]; then
      echo "   ✅ Gold schema location correct (tm-lakehouse-serve)"
    else
      echo "   ⚙️  Fixing gold schema location..."
      docker exec trino trino --execute "DROP SCHEMA IF EXISTS iceberg.gold CASCADE" 2>&1 | grep -v jline > /dev/null || true
      docker exec trino trino --execute "CREATE SCHEMA iceberg.gold WITH (location = 's3://tm-lakehouse-serve/warehouse/gold')" 2>&1 | grep -v jline > /dev/null
      echo "   ✅ Gold schema created with correct location (tm-lakehouse-serve)"
    fi
  else
    echo "   ❌ Trino not ready"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""
  
  # 6. Verify JARs are valid (not corrupted)
  echo "⚡ Verifying JAR integrity..."
  JAR_SIZE=$(docker exec spark du -sm /opt/spark/extra-jars/ 2>/dev/null | cut -f1 || echo "0")
  if [ "$JAR_SIZE" -ge 700 ]; then
    echo "   ✅ JAR directory size: ${JAR_SIZE}MB (expected: ~740MB)"
    echo "   ✅ All pre-baked JARs verified"
  else
    echo "   ❌ JAR directory too small: ${JAR_SIZE}MB (expected: ~740MB)"
    echo "      The JARs may be corrupted. Run: just rebuild"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""
  
  # Summary
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if [ "$ERRORS" -eq 0 ]; then
    echo "✅ ALL CHECKS PASSED - Ready to run pipeline!"
    echo ""
    echo "   Next steps:"
    echo "     just bronze-upload-all    # Upload CSV files to Landing Zone"
    echo "     just silver-all           # Transform to Iceberg tables"
    echo "     just gold                 # Build Gold aggregations"
    echo ""
  else
    echo "❌ $ERRORS CHECK(S) FAILED"
    echo ""
    echo "   To fix:"
    echo "     just rebuild              # Rebuild containers with dependencies"
    echo "     just up                   # Or restart the stack"
    echo ""
    exit 1
  fi

# Initialize Iceberg schemas with correct warehouse locations
init-schemas:
  #!/usr/bin/env bash
  set -euo pipefail
  
  echo ""
  echo "🗂️  INITIALIZING ICEBERG SCHEMAS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  
  # Wait for Trino to be ready
  echo "  ⏳ Waiting for Trino..."
  for i in {1..12}; do
    if curl -s http://localhost:8081/v1/info 2>/dev/null | grep -q "starting.*false"; then
      echo "  ✅ Trino is ready"
      break
    fi
    if [ $i -eq 12 ]; then
      echo "  ❌ Trino not ready. Start the stack first: just up"
      exit 1
    fi
    sleep 5
  done
  echo ""
  
  # Ensure silver schema exists (usually auto-created by Spark)
  echo "  📊 Checking silver schema..."
  SILVER_EXISTS=$(docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg" 2>&1 | grep -c "silver" || echo "0")
  if [ "$SILVER_EXISTS" -gt 0 ]; then
    echo "     ✅ silver schema exists"
  else
    echo "     ⚙️  Creating silver schema..."
    docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3://tm-lakehouse-source-store/warehouse/silver')" 2>&1 | grep -v jline > /dev/null
    echo "     ✅ silver schema created"
  fi
  
  # Ensure gold schema has correct location
  echo "  🏆 Checking gold schema..."
  GOLD_LOCATION=$(docker exec trino trino --execute "SHOW CREATE SCHEMA iceberg.gold" 2>&1 | grep -o "s3://[^']*" || echo "")
  if [ "$GOLD_LOCATION" = "s3://tm-lakehouse-serve/warehouse/gold" ]; then
    echo "     ✅ gold schema location correct (tm-lakehouse-serve)"
  else
    if [ -n "$GOLD_LOCATION" ]; then
      echo "     ⚠️  gold schema has wrong location: $GOLD_LOCATION"
      echo "     ⚙️  Dropping and recreating..."
      docker exec trino trino --execute "DROP SCHEMA IF EXISTS iceberg.gold CASCADE" 2>&1 | grep -v jline > /dev/null || true
    fi
    docker exec trino trino --execute "CREATE SCHEMA iceberg.gold WITH (location = 's3://tm-lakehouse-serve/warehouse/gold')" 2>&1 | grep -v jline > /dev/null
    echo "     ✅ gold schema created with correct location"
  fi
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "✅ SCHEMAS INITIALIZED"
  echo ""
  echo "  Locations:"
  echo "    Silver → s3://tm-lakehouse-source-store/warehouse/silver"
  echo "    Gold   → s3://tm-lakehouse-serve/warehouse/gold"
  echo ""

logs:
  docker compose --env-file config/local.env logs -f --tail=200

ui:
  @echo ""
  @echo "🌐 SERVICE URLS"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  @echo "  Airflow   http://localhost:8080    (admin/admin)"
  @echo "  Trino     http://localhost:8081"
  @echo "  Spark     http://localhost:8082"
  @echo "  MinIO     http://localhost:9001    (minioadmin/minioadmin)"
  @echo "  Superset  http://localhost:8088    (admin/admin)"
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

# ============================================
# SUPERSET COMMANDS
# ============================================

# Check if Superset is ready
superset-status:
  #!/usr/bin/env bash
  echo ""
  echo "📊 SUPERSET STATUS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if docker ps | grep -q "superset"; then
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8088/health 2>/dev/null | grep -q "200"; then
      echo "  ✅ Superset is ready!"
      echo "  📍 URL: http://localhost:8088"
      echo "  🔑 Login: admin / admin"
    else
      echo "  ⏳ Superset is starting (takes ~2-3 minutes first time)..."
      echo "     Check logs: docker logs superset --tail 20"
    fi
  else
    echo "  ❌ Superset is not running"
    echo "     Start with: just up"
  fi
  echo ""

# Open Superset in browser
superset:
  @echo ""
  @echo "📊 Opening Superset..."
  @echo "  URL: http://localhost:8088"
  @echo "  Login: admin / admin"
  @echo ""
  @open http://localhost:8088 2>/dev/null || xdg-open http://localhost:8088 2>/dev/null || echo "  Open manually: http://localhost:8088"

# Show Superset connection string for Trino
superset-trino-connection:
  @echo ""
  @echo "📊 SUPERSET → TRINO CONNECTION"
  @echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  @echo ""
  @echo "  In Superset, go to: Settings → Database Connections → + Database"
  @echo ""
  @echo "  Database Type: Trino"
  @echo "  SQLAlchemy URI:"
  @echo "    trino://trino@trino:8080/iceberg"
  @echo ""
  @echo "  Display Name: TagMarshal Lakehouse"
  @echo ""
  @echo "  Available Schemas:"
  @echo "    - silver (fact_telemetry_event)"
  @echo "    - gold (pace_summary_by_round, signal_quality_rounds, etc.)"
  @echo ""

# ============================================
# STREAMLIT DASHBOARD
# ============================================

# Install dashboard dependencies
dashboard-install:
  #!/usr/bin/env bash
  set -euo pipefail
  echo ""
  echo "📦 INSTALLING DASHBOARD DEPENDENCIES"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  pip install -r dashboard/requirements.txt
  echo ""
  echo "✅ Dependencies installed!"

# Run the Streamlit data quality dashboard
dashboard:
  #!/usr/bin/env bash
  set -euo pipefail
  echo ""
  echo "📊 STARTING DATA QUALITY DASHBOARD"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  echo "  Prerequisites:"
  echo "    - Trino must be running (just trino-status)"
  echo "    - Gold models must be built (just gold)"
  echo ""
  echo "  Opening dashboard at http://localhost:8501"
  echo "  Press Ctrl+C to stop"
  echo ""
  STREAMLIT_TELEMETRY=false streamlit run dashboard/app.py --server.port 8501 --server.headless true --theme.base light

# Run dashboard in background
dashboard-bg:
  #!/usr/bin/env bash
  set -euo pipefail
  echo ""
  echo "📊 STARTING DASHBOARD (BACKGROUND)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  STREAMLIT_TELEMETRY=false nohup streamlit run dashboard/app.py --server.port 8501 --server.headless true --theme.base light > /tmp/streamlit.log 2>&1 &
  echo ""
  echo "✅ Dashboard started in background"
  echo "  📍 URL: http://localhost:8501"
  echo "  📋 Logs: /tmp/streamlit.log"
  echo ""
  sleep 2
  open http://localhost:8501 2>/dev/null || xdg-open http://localhost:8501 2>/dev/null || echo "  Open manually: http://localhost:8501"

# Stop background dashboard
dashboard-stop:
  #!/usr/bin/env bash
  echo ""
  echo "🛑 STOPPING DASHBOARD"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  pkill -f "streamlit run dashboard/app.py" 2>/dev/null || echo "No dashboard process found"
  echo "✅ Dashboard stopped"

# ============================================
# MINIO COMMANDS
# ============================================

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

# View a file from MinIO (CSV, JSON, or text files)
# Usage: just view <bucket>/<path/to/file>
# Examples:
#   just view tm-lakehouse-landing-zone/course_id=americanfalls/ingest_date=2026-01-04/americanfalls.rounds.csv
#   just view tm-lakehouse-observability/gold/run_id=20260104T135009Z/summary.json
view path:
  #!/usr/bin/env bash
  
  # Ensure MinIO alias exists
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null
  
  FILE_PATH="{{path}}"
  
  # Check if file exists
  if ! docker exec minio mc stat "myminio/$FILE_PATH" >/dev/null 2>&1; then
    echo "❌ File not found: $FILE_PATH"
    echo ""
    echo "💡 Try listing files first:"
    echo "   just ls tm-lakehouse-landing-zone"
    echo "   just ls tm-lakehouse-observability/gold"
    exit 1
  fi
  
  # Get file extension
  EXT="${FILE_PATH##*.}"
  
  case "$EXT" in
    json)
      # Pretty-print JSON
      docker exec minio mc cat "myminio/$FILE_PATH" | python3 -m json.tool
      ;;
    csv)
      # Show CSV sample: header + first 5 rows, truncated to 120 chars per line
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      echo "📄 $FILE_PATH"
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      echo ""
      # Download to temp file to avoid SIGPIPE issues
      TMPFILE=$(mktemp)
      docker exec minio mc cat "myminio/$FILE_PATH" > "$TMPFILE" 2>/dev/null
      # Show first 6 lines truncated
      head -6 "$TMPFILE" | cut -c1-120 | sed 's/$/.../g'
      echo ""
      LINES=$(wc -l < "$TMPFILE")
      COLS=$(head -1 "$TMPFILE" | tr ',' '\n' | wc -l)
      rm -f "$TMPFILE"
      echo "📊 File stats: $LINES rows × $COLS columns"
      echo "💡 Use 'just view-all {{path}}' to see entire file"
      ;;
    *)
      # Default: show raw content (first 20 lines, truncated)
      docker exec minio mc cat "myminio/$FILE_PATH" 2>/dev/null | head -20 | cut -c1-200 || true
      ;;
  esac

# View entire file from MinIO (no line limit)
view-all path:
  #!/usr/bin/env bash
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null
  docker exec minio mc cat "myminio/{{path}}"

# List files in a MinIO path
# Usage: just ls <bucket> or just ls <bucket>/<prefix>
# Examples:
#   just ls tm-lakehouse-landing-zone
#   just ls tm-lakehouse-observability/gold
ls path="":
  #!/usr/bin/env bash
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null
  
  if [ -z "{{path}}" ]; then
    echo ""
    echo "📦 MINIO BUCKETS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker exec minio mc ls myminio/
    echo ""
    echo "💡 Use 'just ls <bucket>' to list contents"
  else
    echo ""
    echo "📁 {{path}}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker exec minio mc ls "myminio/{{path}}/" --recursive | head -50
    TOTAL=$(docker exec minio mc ls "myminio/{{path}}/" --recursive 2>/dev/null | wc -l)
    if [ "$TOTAL" -gt 50 ]; then
      echo ""
      echo "... (showing first 50 of $TOTAL files)"
    fi
  fi

# View latest Gold observability summary
gold-obs:
  #!/usr/bin/env bash
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null
  
  LATEST=$(docker exec minio mc ls myminio/tm-lakehouse-observability/gold/ --recursive 2>/dev/null | grep summary.json | tail -1 | awk '{print $NF}')
  
  if [ -z "$LATEST" ]; then
    echo "❌ No Gold observability data found"
    echo "💡 Run 'just gold' first to generate observability data"
    exit 1
  fi
  
  echo ""
  echo "📊 LATEST GOLD RUN"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  docker exec minio mc cat "myminio/tm-lakehouse-observability/gold/$LATEST" | python3 -m json.tool

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

  # Auto-generate topology (ensure new courses are captured immediately)
  just generate-topology
  # Upload and seed topology table
  just seed-topology

# Process ALL courses through Silver ETL
# Mode (sequential/parallel) is controlled by TM_PIPELINE_MODE in config/local.env
silver-all ingest_date="":
  #!/usr/bin/env bash
  set -euo pipefail
  INGEST_DATE="{{ingest_date}}"
  [ -z "$INGEST_DATE" ] && INGEST_DATE=$(date +%Y-%m-%d)
  
  # Read pipeline mode from config (default: sequential for local dev)
  PIPELINE_MODE=$(grep "^TM_PIPELINE_MODE=" config/local.env 2>/dev/null | cut -d= -f2 || echo "sequential")
  
  echo ""
  echo "⚙️  SILVER ETL"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Date: $INGEST_DATE"
  echo "  Mode: $PIPELINE_MODE"
  echo ""
  
  # Get unique courses from CSV files
  declare -a courses=()
  for file in data/*.csv; do
    filename=$(basename "$file")
    course_id=$(echo "$filename" | sed -E 's/\.rounds.*//; s/\..*//')
    # Only add if not already in array (avoid duplicates)
    if [[ ! " ${courses[*]:-} " =~ " ${course_id} " ]]; then
      courses+=("$course_id")
    fi
  done
  
  TOTAL=${#courses[@]}
  echo "  📋 Found $TOTAL unique courses"
  echo ""
  
  START_TIME=$(date +%s)
  COMPLETED=0
  FAILED=0
  
  if [ "$PIPELINE_MODE" = "sequential" ]; then
    # SEQUENTIAL: Trigger one job, wait for completion, then next
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    for i in "${!courses[@]}"; do
      course_id="${courses[$i]}"
    bronze_prefix="course_id=$course_id/ingest_date=$INGEST_DATE/"
      
      echo ""
      echo "  [$((i+1))/$TOTAL] $course_id"
      
      JOB_START=$(date +%s)
      
      # Trigger job
      OUTPUT=$(docker exec airflow airflow dags trigger silver_etl \
      --conf "{\"course_id\":\"$course_id\",\"ingest_date\":\"$INGEST_DATE\",\"bronze_prefix\":\"$bronze_prefix\"}" \
        2>&1 || echo "")
      
      # Extract run_id from output (format: manual__2026-01-04T13:14:46+00:00)
      RUN_ID=$(echo "$OUTPUT" | grep -o "manual__[0-9T:+-]*" | head -1 || echo "")
      
      if [ -z "$RUN_ID" ]; then
        echo "     ❌ Failed to trigger"
        FAILED=$((FAILED + 1))
        continue
      fi
      
      # Wait for this job to complete
      echo -n "     ⏳ Running"
      while true; do
        STATE=$(docker exec airflow airflow dags list-runs -d silver_etl -o plain 2>&1 | \
          grep "$RUN_ID" | awk '{print $3}' | head -1 || echo "running")
        
        if [ "$STATE" = "success" ]; then
          JOB_END=$(date +%s)
          JOB_TIME=$((JOB_END - JOB_START))
          echo ""
          echo "     ✅ Done in ${JOB_TIME}s"
          COMPLETED=$((COMPLETED + 1))
          break
        elif [ "$STATE" = "failed" ]; then
          JOB_END=$(date +%s)
          JOB_TIME=$((JOB_END - JOB_START))
          echo ""
          echo "     ❌ FAILED after ${JOB_TIME}s"
          FAILED=$((FAILED + 1))
          break
        fi
        echo -n "."
        sleep 5
      done
    done
    
  else
    # PARALLEL: Trigger all jobs, then monitor progress
    declare -a run_ids=()
    declare -a statuses=()
    
    echo "  🚀 Triggering all jobs..."
    for i in "${!courses[@]}"; do
      course_id="${courses[$i]}"
      bronze_prefix="course_id=$course_id/ingest_date=$INGEST_DATE/"
      
      OUTPUT=$(docker exec airflow airflow dags trigger silver_etl \
        --conf "{\"course_id\":\"$course_id\",\"ingest_date\":\"$INGEST_DATE\",\"bronze_prefix\":\"$bronze_prefix\"}" \
        2>&1 || echo "")
      
      # Extract run_id from output (format: manual__2026-01-04T13:14:46+00:00)
      RUN_ID=$(echo "$OUTPUT" | grep -o "manual__[0-9T:+-]*" | head -1 || echo "unknown_$i")
      run_ids+=("$RUN_ID")
      statuses+=("running")
      
      echo "     ⏳ $course_id"
      sleep 0.5
    done
    
  echo ""
    echo "  ✅ All $TOTAL jobs triggered"
  echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  📊 PROGRESS (updating every 10s)"
    echo ""
    
    # Monitor until all complete
    while [ $((COMPLETED + FAILED)) -lt $TOTAL ]; do
  for i in "${!courses[@]}"; do
        if [ "${statuses[$i]}" = "running" ]; then
          course_id="${courses[$i]}"
          run_id="${run_ids[$i]}"
          
          STATE=$(docker exec airflow airflow dags list-runs -d silver_etl -o plain 2>&1 | \
            grep "$run_id" | awk '{print $3}' | head -1 || echo "running")
          
          if [ "$STATE" = "success" ]; then
            statuses[$i]="success"
            ELAPSED=$(($(date +%s) - START_TIME))
            COMPLETED=$((COMPLETED + 1))
            echo "     ✅ $course_id completed (${ELAPSED}s elapsed)"
          elif [ "$STATE" = "failed" ]; then
            statuses[$i]="failed"
            ELAPSED=$(($(date +%s) - START_TIME))
            FAILED=$((FAILED + 1))
            echo "     ❌ $course_id FAILED (${ELAPSED}s elapsed)"
          fi
        fi
      done
      
      RUNNING=$((TOTAL - COMPLETED - FAILED))
      if [ $RUNNING -gt 0 ]; then
        ELAPSED=$(($(date +%s) - START_TIME))
        printf "\r     ⏳ %d running, %d done, %d failed (%dm %ds)  " \
          $RUNNING $COMPLETED $FAILED $((ELAPSED / 60)) $((ELAPSED % 60))
        sleep 10
      fi
    done
  echo ""
  fi
  
  END_TIME=$(date +%s)
  TOTAL_TIME=$((END_TIME - START_TIME))
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  ✅ SILVER ETL COMPLETE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  echo "  📊 Summary:"
  echo "     Total:     $TOTAL courses"
  echo "     Success:   $COMPLETED"
  echo "     Failed:    $FAILED"
  echo "     Duration:  ${TOTAL_TIME}s (~$((TOTAL_TIME / 60))m $((TOTAL_TIME % 60))s)"
  echo ""
  
  # Show row counts from Trino
  echo "  📈 Row Counts:"
  docker exec trino trino --execute \
    "SELECT course_id, count(*) as rows FROM iceberg.silver.fact_telemetry_event GROUP BY course_id ORDER BY course_id" \
    2>&1 | grep -v "jline\|WARNING" | while read line; do
    echo "     $line"
  done
  echo ""
  echo "  📍 Locations:"
  echo "     Table: iceberg.silver.fact_telemetry_event"
  echo "     MinIO: http://localhost:9001/browser/tm-lakehouse-source-store/warehouse/silver/"
  echo ""
  echo "  💡 Config: TM_PIPELINE_MODE=$PIPELINE_MODE (config/local.env)"
  echo ""
  
  # Auto-generate topology
  just generate-topology
  # Upload and seed topology table
  just seed-topology

# Generate topology mapping from processed data
generate-topology:
  #!/usr/bin/env bash
  set -euo pipefail
  
  echo ""
  echo "🧩 GENERATING TOPOLOGY"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  
  # Run generator in Spark container and capture output to tmp file
  echo "  ⏳ Analyzing Silver data..."
  docker exec spark python3 /opt/tagmarshal/pipeline/scripts/generate_topology.py --output /tmp/topology.csv 2>&1 | grep -v "NativeCodeLoader\|SLF4J\|MetricsConfig" || true

  # Copy result back to host
  echo "  📥 Updating local topology file..."
  docker cp spark:/tmp/topology.csv pipeline/silver/seeds/dim_facility_topology.csv
  
  echo ""
  echo "  ✅ Topology updated: pipeline/silver/seeds/dim_facility_topology.csv"
  echo "  💡 You can now edit this file to rename units (e.g. 'Unit 1' → 'Red Course')"
  echo ""

# One-shot helper: regenerate + seed topology
topology-refresh:
  #!/usr/bin/env bash
  set -euo pipefail
  just generate-topology
  just seed-topology

# Seed the topology data into an Iceberg table (for Trino/DBeaver visibility)
seed-topology:
  #!/usr/bin/env bash
  set -euo pipefail

  echo ""
  echo "🌱 SEEDING TOPOLOGY TABLE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""

  # 1. Upload CSV to MinIO
  echo "  📤 Uploading CSV to MinIO..."
  cat pipeline/silver/seeds/dim_facility_topology.csv | docker exec -i minio mc pipe myminio/tm-lakehouse-source-store/seeds/dim_facility_topology.csv > /dev/null

  # 2. Run Spark job to load it into Iceberg
  echo "  ❄️  Loading into Iceberg table (iceberg.silver.dim_facility_topology)..."
  docker exec spark python3 /opt/tagmarshal/pipeline/scripts/seed_topology.py

  echo ""
  echo "  ✅ Seeding complete"
  echo "  📍 Query: SELECT * FROM iceberg.silver.dim_facility_topology"
  echo ""

# Seed the course profile (simple record of course types / notes)
seed-course-profile:
  #!/usr/bin/env bash
  set -euo pipefail

  echo ""
  echo "🗂️  SEEDING COURSE PROFILE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""

  # 1. Upload CSV to MinIO
  echo "  📤 Uploading CSV to MinIO..."
  cat pipeline/silver/seeds/dim_course_profile.csv | docker exec -i minio mc pipe myminio/tm-lakehouse-source-store/seeds/dim_course_profile.csv > /dev/null

  # 2. Run Spark job to load it into Iceberg (MERGE / upsert)
  echo "  ❄️  Loading into Iceberg table (iceberg.silver.dim_course_profile)..."
  docker exec spark python3 /opt/tagmarshal/pipeline/scripts/seed_course_profile.py

  echo ""
  echo "  ✅ Seeding complete"
  echo "  📍 Query: SELECT * FROM iceberg.silver.dim_course_profile"
  echo ""


# Run Gold dbt models
gold:
  #!/usr/bin/env bash
  set -euo pipefail
  
  echo ""
  echo "🏆 GOLD DBT MODELS"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  echo "  Models to build:"
  echo "    • gold.pace_summary_by_round"
  echo "    • gold.signal_quality_rounds"
  echo "    • gold.device_health_errors"
  echo "    • gold.course_configuration_analysis"
  echo "    • gold.critical_column_gaps"
  echo "    • gold.data_quality_overview"
  echo ""
  
  START_TIME=$(date +%s)
  
  # Trigger gold_dbt DAG and capture run_id
  echo "  🚀 Triggering dbt models..."
  OUTPUT=$(docker exec airflow airflow dags trigger gold_dbt 2>&1 || echo "")
  
  # Extract run_id from output (format: manual__2026-01-04T13:14:46+00:00)
  RUN_ID=$(echo "$OUTPUT" | grep -o "manual__[0-9T:+-]*" | head -1 || echo "")
  
  if [ -z "$RUN_ID" ]; then
    echo "  ❌ Failed to trigger gold_dbt DAG"
    exit 1
  fi
  
  echo "  ✅ Triggered (run_id: ${RUN_ID:0:35}...)"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo -n "  ⏳ Running"
  
  # Wait for completion
  while true; do
    STATE=$(docker exec airflow airflow dags list-runs -d gold_dbt -o plain 2>&1 | \
      grep "$RUN_ID" | awk '{print $3}' | head -1 || echo "running")
    
    if [ "$STATE" = "success" ]; then
      END_TIME=$(date +%s)
      DURATION=$((END_TIME - START_TIME))
      echo ""
      echo "  ✅ Done in ${DURATION}s"
      break
    elif [ "$STATE" = "failed" ]; then
      END_TIME=$(date +%s)
      DURATION=$((END_TIME - START_TIME))
      echo ""
      echo "  ❌ FAILED after ${DURATION}s"
      echo ""
      echo "  Check logs: http://localhost:8080/dags/gold_dbt/grid"
      exit 1
    fi
    echo -n "."
    sleep 5
  done
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  ✅ GOLD BUILD COMPLETE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  
  # Show row counts for gold tables
  echo "  📈 Gold Table Row Counts:"
  for table in pace_summary_by_round signal_quality_rounds device_health_errors course_configuration_analysis critical_column_gaps data_quality_overview; do
    ROWS=$(docker exec trino trino --execute \
      "SELECT count(*) FROM iceberg.gold.$table" 2>&1 | grep -v "jline\|WARNING" | tr -d '"' || echo "0")
    echo "     $table: $ROWS rows"
  done
  echo ""
  echo "  📍 Locations:"
  echo "     MinIO: http://localhost:9001/browser/tm-lakehouse-serve/warehouse/gold/"
  echo "     Query: SELECT * FROM iceberg.gold.pace_summary_by_round"
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
  docker exec -i airflow-postgres psql -U airflow -d airflow < pipeline/infrastructure/database/001_create_ingestion_log.sql 2>&1 | grep -v "NOTICE\|already exists" || true
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
  python3 pipeline/scripts/backfill.py {{layer}} --dry-run
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
  
  python3 pipeline/scripts/backfill.py silver $COURSE_ARG --batch-size {{batch_size}}
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
