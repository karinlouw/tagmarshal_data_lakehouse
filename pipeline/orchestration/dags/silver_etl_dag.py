"""Silver ETL DAG - Spark job to transform Landing Zone CSV → Iceberg.

Configuration is driven by environment variables (see config/local.env and config/aws.env):
- TM_SPARK_MASTER: Spark master mode (local[N], yarn, etc.)
- TM_SPARK_DRIVER_MEMORY: Driver memory allocation
- TM_SPARK_EXECUTOR_MEMORY: Executor memory allocation
- TM_SPARK_SHUFFLE_PARTITIONS: Number of shuffle partitions
- TM_SPARK_ADAPTIVE_ENABLED: Enable adaptive query execution
- TM_SPARK_UI_ENABLED: Enable/disable Spark UI
"""

from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timedelta
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from tm_lakehouse.config import TMConfig

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =============================================================================
# SPARK CONFIGURATION (read from environment with sensible defaults)
# =============================================================================
# These values come from config/local.env or config/aws.env
# Defaults are conservative for local development
SPARK_CONFIG = {
    "master": os.getenv("TM_SPARK_MASTER", "local[2]"),
    "driver_memory": os.getenv("TM_SPARK_DRIVER_MEMORY", "1g"),
    "executor_memory": os.getenv("TM_SPARK_EXECUTOR_MEMORY", "1g"),
    "shuffle_partitions": os.getenv("TM_SPARK_SHUFFLE_PARTITIONS", "8"),
    "adaptive_enabled": os.getenv("TM_SPARK_ADAPTIVE_ENABLED", "true").lower()
    == "true",
    "ui_enabled": os.getenv("TM_SPARK_UI_ENABLED", "false").lower() == "true",
}


def log_to_registry(
    course_id: str,
    ingest_date: str,
    status: str,
    rows: int = 0,
    error: str = None,
    dag_run_id: str = None,
):
    """Log Silver ETL result to ingestion registry for resumability tracking."""
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        error_value = f"'{error[:500]}'" if error else "NULL"

        sql = f"""
            INSERT INTO ingestion_log 
                (course_id, ingest_date, layer, filename, status, rows_processed, 
                 error_message, dag_run_id, completed_at)
            VALUES 
                ('{course_id}', '{ingest_date}', 'silver', '{course_id}_{ingest_date}', 
                 '{status}', {rows}, {error_value}, '{dag_run_id or ""}', NOW())
            ON CONFLICT (filename, ingest_date, layer) 
            DO UPDATE SET 
                status = '{status}',
                rows_processed = {rows},
                error_message = {error_value},
                completed_at = NOW(),
                retry_count = ingestion_log.retry_count + 1
        """
        hook.run(sql)
    except Exception as e:
        print(f"Warning: Failed to log to registry: {e}")


def run_spark_etl(**context):
    """Run Spark ETL job to transform Landing Zone CSV into Silver Iceberg table."""
    cfg = TMConfig.from_env()

    # Get params from DAG trigger conf
    run_conf = context.get("dag_run").conf or {}

    course_id = run_conf.get("course_id")
    ingest_date = run_conf.get("ingest_date")
    bronze_prefix = run_conf.get("bronze_prefix")

    if not course_id or not ingest_date or not bronze_prefix:
        raise ValueError(
            f"Missing required params. Got: course_id={course_id}, ingest_date={ingest_date}, bronze_prefix={bronze_prefix}"
        )

    # Log configuration being used
    env = os.getenv("TM_ENV", "unknown")
    print(f"\n{'='*60}")
    print("⚙️  SILVER ETL: Spark Transform")
    print(f"{'='*60}")
    print(f"  Course:      {course_id}")
    print(f"  Date:        {ingest_date}")
    print(f"  Source:      s3://{cfg.bucket_landing}/{bronze_prefix}")
    print(f"  Environment: {env}")
    print(f"  Spark Config:")
    print(f"    Master:     {SPARK_CONFIG['master']}")
    print(f"    Driver Mem: {SPARK_CONFIG['driver_memory']}")
    print(f"    Exec Mem:   {SPARK_CONFIG['executor_memory']}")
    print(f"    Partitions: {SPARK_CONFIG['shuffle_partitions']}")
    print("")

    # Build the spark-submit command
    # Use local pre-baked JARs (no Maven downloads needed - much faster and more reliable!)
    jars_path = "/opt/spark/extra-jars"
    jars = ",".join(
        [
            f"{jars_path}/hadoop-aws-3.3.4.jar",
            f"{jars_path}/aws-java-sdk-bundle-1.12.262.jar",
            f"{jars_path}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar",
            f"{jars_path}/bundle-2.20.18.jar",
            f"{jars_path}/wildfly-openssl-1.0.7.Final.jar",
            f"{jars_path}/eventstream-1.0.1.jar",
        ]
    )

    # Build command with config-driven settings
    docker_env = [
        "-e",
        f"AWS_REGION={cfg.s3_region}",
        "-e",
        f"TM_BUCKET_LANDING={cfg.bucket_landing}",
        "-e",
        f"TM_BUCKET_SOURCE={cfg.bucket_source}",
        "-e",
        f"TM_BUCKET_QUARANTINE={cfg.bucket_quarantine}",
        "-e",
        f"TM_BUCKET_OBSERVABILITY={cfg.bucket_observability}",
        "-e",
        f"TM_ICEBERG_WAREHOUSE_SILVER={cfg.iceberg_warehouse_silver}",
        "-e",
        f"TM_DB_SILVER={cfg.db_silver}",
        "-e",
        f"TM_S3_REGION={cfg.s3_region}",
    ]
    if cfg.s3_endpoint:
        docker_env += ["-e", f"TM_S3_ENDPOINT={cfg.s3_endpoint}"]
    if cfg.s3_access_key:
        docker_env += ["-e", f"TM_S3_ACCESS_KEY={cfg.s3_access_key}"]
    if cfg.s3_secret_key:
        docker_env += ["-e", f"TM_S3_SECRET_KEY={cfg.s3_secret_key}"]

    cmd = [
        "docker",
        "exec",
        *docker_env,
        "spark",
        "/opt/spark/bin/spark-submit",
        "--master",
        SPARK_CONFIG["master"],
        "--driver-memory",
        SPARK_CONFIG["driver_memory"],
        "--conf",
        f"spark.executor.memory={SPARK_CONFIG['executor_memory']}",
        "--conf",
        f"spark.sql.adaptive.enabled={str(SPARK_CONFIG['adaptive_enabled']).lower()}",
        "--conf",
        "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf",
        f"spark.sql.shuffle.partitions={SPARK_CONFIG['shuffle_partitions']}",
        "--conf",
        "spark.driver.maxResultSize=512m",
        "--conf",
        f"spark.ui.enabled={str(SPARK_CONFIG['ui_enabled']).lower()}",
        "--jars",
        jars,
        "/opt/tagmarshal/pipeline/silver/etl.py",
        "--course-id",
        course_id,
        "--ingest-date",
        ingest_date,
        "--bronze-prefix",
        bronze_prefix,
    ]

    print("  Running Spark job...")
    print(f"  Command: {' '.join(cmd[:10])}...")
    print("")

    # Run the command
    result = subprocess.run(cmd, capture_output=True, text=True)

    def _filter_spark_noise(text: str) -> list[str]:
        """
        Keep high-signal ETL output, drop Spark/Java INFO spam.

        Spark commonly prefixes logs like: '26/01/07 14:30:03 INFO ...'
        We also drop common SLF4J + Hadoop native loader noise.
        """
        if not text:
            return []

        drop_patterns = [
            re.compile(r"^\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+(INFO|WARN|ERROR)\b"),
            re.compile(r"^SLF4J:"),
        ]
        drop_substrings = (
            "NativeCodeLoader",
            "MetricsConfig",
            "SparkUI",
            "BlockManager",
            "Executor:",
            "SparkContext:",
            "ResourceProfile",
        )

        kept: list[str] = []
        for raw in text.splitlines():
            line = raw.rstrip()
            if not line:
                continue
            if any(p.match(line) for p in drop_patterns):
                continue
            if any(s in line for s in drop_substrings):
                continue
            kept.append(line)
        return kept

    # Print output
    if result.stdout:
        print("  [Spark stdout]")
        lines = _filter_spark_noise(result.stdout)
        tail = lines[-60:] if lines else []
        for line in tail:
            print(f"    {line}")

    if result.returncode != 0:
        print(f"\n  ❌ Spark job failed with exit code {result.returncode}")
        if result.stderr:
            print("  [Spark stderr]")
            lines = _filter_spark_noise(result.stderr)
            # Keep a slightly longer tail for exceptions/stacktraces
            tail = lines[-120:] if lines else []
            for line in tail:
                print(f"    {line}")

        # Log failure to registry for resumability
        # Preserve raw stderr tail for the exception message (even if logs are noisy)
        error_msg = result.stderr[-500:] if result.stderr else "No error message"
        log_to_registry(
            course_id,
            ingest_date,
            "failed",
            error=error_msg,
            dag_run_id=context.get("dag_run").run_id,
        )

        raise RuntimeError(f"Spark job failed: {error_msg}")

    # Extract row count from output if available
    rows_processed = 0
    for line in result.stdout.split("\n"):
        if "Appended" in line and "rows" in line:
            try:
                # Parse "→ Appended 67,660 rows"
                match = re.search(r"(\d[\d,]*)\s*rows", line)
                if match:
                    rows_processed = int(match.group(1).replace(",", ""))
            except:
                pass

    # Log success to registry for resumability tracking
    log_to_registry(
        course_id,
        ingest_date,
        "success",
        rows=rows_processed,
        dag_run_id=context.get("dag_run").run_id,
    )

    print("")
    print("✅ SILVER ETL COMPLETE")
    print(f"{'='*60}")
    print(f"  Course:      {course_id}")
    print(f"  Ingest Date: {ingest_date}")
    print(f"  Rows:        {rows_processed:,}")
    print("")
    print("  📍 Data Locations:")
    print(
        f"     Landing:  http://localhost:9001/browser/tm-lakehouse-landing-zone/course_id%3D{course_id}/"
    )
    print(
        f"     Source:   http://localhost:9001/browser/tm-lakehouse-source-store/warehouse/silver/"
    )
    print(f"{'='*60}\n")

    return {"course_id": course_id, "status": "success", "rows": rows_processed}


with DAG(
    dag_id="silver_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    # Fail-fast defaults for local dev: Airflow's implicit default retry_delay is 5 minutes,
    # which makes failures look like they "hang" for a long time. Keep a retry, but shorten delay.
    default_args={
        "retries": int(os.getenv("TM_AIRFLOW_SILVER_RETRIES", "1")),
        "retry_delay": timedelta(
            seconds=int(os.getenv("TM_AIRFLOW_SILVER_RETRY_DELAY_SECONDS", "30"))
        ),
    },
    doc_md="""
### Silver ETL (Spark)
Transforms Landing Zone CSV into Silver Iceberg table (long format).

**Trigger params:**
- `course_id`: e.g. `americanfalls`
- `ingest_date`: `YYYY-MM-DD`
- `bronze_prefix`: e.g. `course_id=americanfalls/ingest_date=2025-12-16/`

**Output:**
- Silver Iceberg table: `iceberg.silver.fact_telemetry_event`
- MinIO: http://localhost:9001/browser/tm-lakehouse-source-store/warehouse/silver/
""",
):
    PythonOperator(
        task_id="spark_silver_etl",
        python_callable=run_spark_etl,
    )
