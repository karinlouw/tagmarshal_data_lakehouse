"""Gold dbt DAG - builds Gold analytical models from Silver using dbt.

Observability: After dbt run, artifacts are uploaded to tm-lakehouse-observability/gold/
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def upload_gold_observability(**context):
    """Upload dbt artifacts to observability bucket after Gold build."""
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dag_run_id = context.get("dag_run").run_id if context.get("dag_run") else "unknown"

    # MinIO/S3 config
    s3_endpoint = os.environ.get("TM_S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.environ.get("TM_S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.environ.get("TM_S3_SECRET_KEY", "minioadmin")
    bucket_obs = os.environ.get("TM_BUCKET_OBSERVABILITY", "tm-lakehouse-observability")

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
    )

    # We keep separate target dirs to avoid artifacts clobbering each other:
    # - dbt run writes run_results + manifest
    # - dbt test writes its own run_results
    # - dbt docs generate writes index.html + catalog + manifest
    dbt_target_run = "/opt/tagmarshal/pipeline/gold/target-run"
    dbt_target_test = "/opt/tagmarshal/pipeline/gold/target-test"
    dbt_target_docs = "/opt/tagmarshal/pipeline/gold/target-docs"
    obs_prefix = f"gold/run_id={run_id}"

    print(f"\n{'='*60}")
    print("📊 GOLD OBSERVABILITY")
    print(f"{'='*60}")
    print(f"  Run ID: {run_id}")
    print(f"  DAG Run: {dag_run_id}")
    print("")

    summary: dict = {
        "run_id": run_id,
        "dag_run_id": dag_run_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "models_success": 0,
        "models_error": 0,
        "tests_pass": 0,
        "tests_fail": 0,
        "tests_warn": 0,
        "models": [],
    }

    # -----------------------
    # dbt run artifacts
    # -----------------------
    run_results_path = f"{dbt_target_run}/run_results.json"
    if os.path.exists(run_results_path):
        with open(run_results_path, "r") as f:
            run_results = json.load(f)

        models = run_results.get("results", [])
        summary["dbt_version"] = run_results.get("metadata", {}).get("dbt_version")
        summary["elapsed_time"] = run_results.get("elapsed_time")
        summary["models_success"] = sum(
            1 for m in models if m.get("status") == "success"
        )
        summary["models_error"] = sum(1 for m in models if m.get("status") == "error")
        total_time = sum(m.get("execution_time", 0) for m in models)
        summary["total_execution_time_sec"] = round(total_time, 2)
        summary["models"] = [
            {
                "name": m.get("unique_id", "")
                .replace("model.tagmarshal_lakehouse.", "")
                .replace("model.tagmarshal.", ""),
                "status": m.get("status"),
                "execution_time_sec": round(m.get("execution_time", 0), 2),
                "rows_affected": m.get("adapter_response", {}).get("rows_affected"),
            }
            for m in models
        ]

        results_key = f"{obs_prefix}/run/run_results.json"
        s3.upload_file(run_results_path, bucket_obs, results_key)
        print(f"  ✅ Run results: s3://{bucket_obs}/{results_key}")
    else:
        print(f"  ⚠️  No dbt run_results.json found at {run_results_path}")

    manifest_path = f"{dbt_target_run}/manifest.json"
    if os.path.exists(manifest_path):
        manifest_key = f"{obs_prefix}/run/manifest.json"
        s3.upload_file(manifest_path, bucket_obs, manifest_key)
        print(f"  ✅ Run manifest: s3://{bucket_obs}/{manifest_key}")

    # -----------------------
    # dbt test artifacts
    # -----------------------
    test_results_path = f"{dbt_target_test}/run_results.json"
    if os.path.exists(test_results_path):
        with open(test_results_path, "r") as f:
            test_results = json.load(f)

        results = test_results.get("results", [])
        summary["tests_pass"] = sum(1 for r in results if r.get("status") == "pass")
        summary["tests_fail"] = sum(1 for r in results if r.get("status") == "fail")
        summary["tests_warn"] = sum(1 for r in results if r.get("status") == "warn")

        test_key = f"{obs_prefix}/test/run_results.json"
        s3.upload_file(test_results_path, bucket_obs, test_key)
        print(f"  ✅ Test results: s3://{bucket_obs}/{test_key}")
    else:
        print(f"  ⚠️  No dbt test run_results.json found at {test_results_path}")

    # -----------------------
    # dbt docs artifacts
    # -----------------------
    docs_files = {
        "index.html": "text/html",
        "catalog.json": "application/json",
        "manifest.json": "application/json",
    }
    docs_uploaded = 0
    for filename, content_type in docs_files.items():
        local_path = f"{dbt_target_docs}/{filename}"
        if not os.path.exists(local_path):
            continue
        key = f"{obs_prefix}/docs/{filename}"
        with open(local_path, "rb") as f:
            s3.put_object(
                Bucket=bucket_obs,
                Key=key,
                Body=f.read(),
                ContentType=content_type,
            )
        docs_uploaded += 1
        print(f"  ✅ Docs: s3://{bucket_obs}/{key}")

    if docs_uploaded:
        summary["docs_index_key"] = f"{obs_prefix}/docs/index.html"
        summary["docs_url"] = (
            f"http://localhost:9001/browser/{bucket_obs}/{obs_prefix}/docs/index.html"
        )

    # Upload summary last (includes run + test + docs pointers)
    summary_key = f"{obs_prefix}/summary.json"
    s3.put_object(
        Bucket=bucket_obs,
        Key=summary_key,
        Body=json.dumps(summary, indent=2),
        ContentType="application/json",
    )
    print(f"  ✅ Summary: s3://{bucket_obs}/{summary_key}")

    print(f"\n{'='*60}")
    print(
        f"  📍 Observability: http://localhost:9001/browser/{bucket_obs}/{obs_prefix}/"
    )
    print(f"{'='*60}\n")

    return {
        "run_id": run_id,
        "models_success": summary.get("models_success", 0),
        "models_error": summary.get("models_error", 0),
        "tests_pass": summary.get("tests_pass", 0),
        "tests_fail": summary.get("tests_fail", 0),
        "tests_warn": summary.get("tests_warn", 0),
    }


with DAG(
    dag_id="gold_dbt",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    doc_md="""
### Gold (dbt)
Builds Gold analytical tables from Silver using dbt.

Run after the Silver quality gate passes.

**Models built:**
- Gold analytical models under `pipeline/gold/models/gold/` (selector: `path:models/gold`)
- Silver-normalized exploration models under `pipeline/gold/models/silver_normalized/`

**Output:**
- Gold Iceberg tables: http://localhost:9001/browser/tm-lakehouse-serve/warehouse/gold/
- Silver Iceberg tables/views: http://localhost:9001/browser/tm-lakehouse-serve/warehouse/silver/
- Observability: http://localhost:9001/browser/tm-lakehouse-observability/gold/
""",
):
    # Install dbt-trino and run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command="""
echo ""
echo "🏆 GOLD DBT MODELS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Install dbt-trino if not already installed
pip install --quiet dbt-trino==1.8.2 2>/dev/null || true

# Change to dbt project directory
cd /opt/tagmarshal/pipeline/gold

# Set profiles directory
export DBT_PROFILES_DIR=/opt/tagmarshal/pipeline/gold

echo "  ⏳ Installing dbt dependencies..."
dbt deps --quiet 2>/dev/null || dbt deps

echo ""
echo "  ⏳ Running Gold models..."
# NOTE: Iceberg REST catalog uses SQLite by default in local dev, which can lock under concurrency.
# Running dbt single-threaded avoids SQLITE_BUSY during catalog updates (e.g., temp table swaps).
dbt run --threads 1 --target-path target-run --select path:models/silver_normalized path:models/gold

echo ""
echo "  🧊 Ensuring Iceberg partitioning (course_id) on key Gold tables..."
dbt run-operation apply_gold_course_partitioning --args '{"models":["fact_rounds","fact_round_hole_performance","course_rounds_by_month","course_start_hole_distribution"]}'

echo ""
echo "  📚 Generating dbt docs..."
dbt docs generate --threads 1 --target-path target-docs

echo ""
echo "  🧪 Running dbt tests (strict)..."
dbt test --threads 1 --target-path target-test --select path:models/silver_normalized path:models/gold

echo ""
echo "✅ DBT RUN COMPLETE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        """,
    )

    # Upload observability artifacts
    upload_obs = PythonOperator(
        task_id="upload_observability",
        python_callable=upload_gold_observability,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run >> upload_obs
