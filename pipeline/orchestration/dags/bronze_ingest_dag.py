"""Bronze ingest DAG - uploads local CSV to Landing Zone with validation and idempotency."""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from bronze.ingest import upload_csv_to_bronze
from tm_lakehouse.config import TMConfig
from tm_lakehouse.observability import write_run_summary
from tm_lakehouse.registry import (
    compute_file_hash,
    is_already_ingested,
    register_ingestion_skipped,
    register_ingestion_start,
    register_ingestion_success,
    register_ingestion_failure,
)


def bronze_upload_task(**context):
    """PythonOperator callable: validates and uploads CSV to Landing Zone with registry tracking."""
    cfg = TMConfig.from_env()

    # Get params from DAG trigger conf or defaults
    run_conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    params = context.get("params") or {}

    course_id = run_conf.get("course_id") or params.get("course_id")
    local_path = run_conf.get("local_path") or params.get("local_path")
    ingest_date = (
        run_conf.get("ingest_date")
        or params.get("ingest_date")
        or datetime.now().strftime("%Y-%m-%d")
    )

    filename = os.path.basename(local_path) if local_path else None
    dag_run_id = context.get("run_id")

    if not course_id:
        raise ValueError("Missing required param: course_id")
    if not local_path:
        raise ValueError("Missing required param: local_path")

    # Clean terminal output
    print(f"\n{'='*60}")
    print(f"📦 BRONZE UPLOAD: {course_id}")
    print(f"{'='*60}")
    print(f"  File: {local_path}")
    print(f"  Date: {ingest_date}")

    # Check registry for prior ingestion
    if is_already_ingested(filename, ingest_date, "bronze"):
        print(f"  ⏭️  SKIP: Already ingested (registry)")
        register_ingestion_skipped(
            filename, course_id, ingest_date, "bronze", "Already in registry"
        )
        print(f"{'='*60}\n")
        return {"bucket": None, "key": None, "row_count": 0, "skipped": True}

    # Register ingestion start
    log_id = register_ingestion_start(
        filename, course_id, ingest_date, "bronze", dag_run_id, "upload_csv_to_bronze"
    )
    file_hash = compute_file_hash(local_path)
    file_size = os.path.getsize(local_path) if os.path.exists(local_path) else None

    try:
        result = upload_csv_to_bronze(
            cfg,
            course_id=course_id,
            local_path=local_path,
            ingest_date=ingest_date,
            skip_if_exists=True,
        )

        if result.skipped:
            # File exists in S3 but wasn't in registry - register it now
            if log_id:
                register_ingestion_success(
                    log_id,
                    result.row_count,
                    f"s3://{result.bucket}/{result.key}",
                    file_size,
                    file_hash,
                )
            print(f"  ⏭️  SKIP: File exists in S3")
        else:
            # Successful upload - register it
            s3_path = f"s3://{result.bucket}/{result.key}"
            if log_id:
                register_ingestion_success(
                    log_id, result.row_count, s3_path, file_size, file_hash
                )
            print(f"  ✅ SUCCESS: {result.row_count:,} rows → {s3_path}")

            write_run_summary(
                cfg,
                stage="bronze",
                run_id=context["run_id"],
                payload={
                    "course_id": course_id,
                    "local_path": local_path,
                    "bucket": result.bucket,
                    "key": result.key,
                    "row_count": result.row_count,
                    "file_hash": file_hash,
                    "status": "ok",
                },
            )

        print(f"{'='*60}\n")
        return {
            "bucket": result.bucket,
            "key": result.key,
            "row_count": result.row_count,
            "skipped": result.skipped,
        }

    except Exception as e:
        if log_id:
            register_ingestion_failure(log_id, str(e))
        print(f"  ❌ FAILED: {e}")
        print(f"{'='*60}\n")
        raise


with DAG(
    dag_id="bronze_ingest",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},  # Retry once on failure (resumability)
    doc_md="""
### Bronze Ingest
Uploads a local CSV to Landing Zone bucket.

**Trigger params:**
- `course_id`: e.g. `americanfalls`
- `local_path`: e.g. `/opt/tagmarshal/input/americanfalls.rounds.csv`
- `ingest_date` (optional): `YYYY-MM-DD` (defaults to today)

**Idempotency:** Skips upload if file already exists in Landing Zone.
""",
):
    PythonOperator(task_id="upload_csv_to_bronze", python_callable=bronze_upload_task)
