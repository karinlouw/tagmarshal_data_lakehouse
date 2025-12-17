"""Silver ETL DAG - Spark job to transform Landing Zone CSV → Iceberg."""

from __future__ import annotations

import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_spark_etl(**context):
    """Run Spark ETL job to transform Landing Zone CSV into Silver Iceberg table."""
    # Get params from DAG trigger conf
    run_conf = context.get("dag_run").conf or {}

    course_id = run_conf.get("course_id")
    ingest_date = run_conf.get("ingest_date")
    bronze_prefix = run_conf.get("bronze_prefix")

    if not course_id or not ingest_date or not bronze_prefix:
        raise ValueError(
            f"Missing required params. Got: course_id={course_id}, ingest_date={ingest_date}, bronze_prefix={bronze_prefix}"
        )

    print(f"\n{'='*60}")
    print("⚙️  SILVER ETL: Spark Transform")
    print(f"{'='*60}")
    print(f"  Course: {course_id}")
    print(f"  Date:   {ingest_date}")
    print(f"  Source: s3://tm-lakehouse-landing-zone/{bronze_prefix}")
    print("")

    # Build the spark-submit command
    cmd = [
        "docker",
        "exec",
        "-e",
        "AWS_REGION=us-east-1",
        "-e",
        "TM_BUCKET_LANDING=tm-lakehouse-landing-zone",
        "-e",
        "TM_BUCKET_SOURCE=tm-lakehouse-source-store",
        "-e",
        "TM_ICEBERG_WAREHOUSE_SILVER=s3a://tm-lakehouse-source-store/warehouse",
        "spark",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[*]",
        "--conf",
        "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy-cache -Divy.home=/tmp/ivy-home",
        "--packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,software.amazon.awssdk:bundle:2.20.18",
        "/opt/tagmarshal/jobs/spark/silver_etl.py",
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

    # Print output
    if result.stdout:
        print("  [Spark stdout]")
        for line in result.stdout.split("\n")[-30:]:  # Last 30 lines
            print(f"    {line}")

    if result.returncode != 0:
        print(f"\n  ❌ Spark job failed with exit code {result.returncode}")
        if result.stderr:
            print("  [Spark stderr]")
            for line in result.stderr.split("\n")[-20:]:  # Last 20 lines
                print(f"    {line}")
        raise RuntimeError(
            f"Spark job failed: {result.stderr[-500:] if result.stderr else 'No error message'}"
        )

    print("")
    print("✅ SILVER ETL COMPLETE")
    print(f"{'='*60}")
    print(f"  Course:      {course_id}")
    print(f"  Ingest Date: {ingest_date}")
    print("")
    print("  📍 Data Locations:")
    print(
        f"     Landing:  http://localhost:9001/browser/tm-lakehouse-landing-zone/course_id%3D{course_id}/"
    )
    print(
        f"     Source:   http://localhost:9001/browser/tm-lakehouse-source-store/warehouse/silver/"
    )
    print(f"{'='*60}\n")

    return {"course_id": course_id, "status": "success"}


with DAG(
    dag_id="silver_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
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
