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

    dbt_target = "/opt/tagmarshal/pipeline/gold/target"
    obs_prefix = f"gold/run_id={run_id}"

    print(f"\n{'='*60}")
    print("📊 GOLD OBSERVABILITY")
    print(f"{'='*60}")
    print(f"  Run ID: {run_id}")
    print(f"  DAG Run: {dag_run_id}")
    print("")

    # Upload run_results.json (contains timing, row counts, success/failure)
    run_results_path = f"{dbt_target}/run_results.json"
    if os.path.exists(run_results_path):
        with open(run_results_path, "r") as f:
            run_results = json.load(f)

        # Extract summary stats
        models = run_results.get("results", [])
        success_count = sum(1 for m in models if m.get("status") == "success")
        error_count = sum(1 for m in models if m.get("status") == "error")
        total_time = sum(m.get("execution_time", 0) for m in models)

        # Create summary
        summary = {
            "run_id": run_id,
            "dag_run_id": dag_run_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "dbt_version": run_results.get("metadata", {}).get("dbt_version"),
            "elapsed_time": run_results.get("elapsed_time"),
            "models_success": success_count,
            "models_error": error_count,
            "total_execution_time_sec": round(total_time, 2),
            "models": [
                {
                    "name": m.get("unique_id", "").replace("model.tagmarshal.", ""),
                    "status": m.get("status"),
                    "execution_time_sec": round(m.get("execution_time", 0), 2),
                    "rows_affected": m.get("adapter_response", {}).get("rows_affected"),
                }
                for m in models
            ],
        }

        # Upload summary
        summary_key = f"{obs_prefix}/summary.json"
        s3.put_object(
            Bucket=bucket_obs,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json",
        )
        print(f"  ✅ Summary: s3://{bucket_obs}/{summary_key}")

        # Upload full run_results.json
        results_key = f"{obs_prefix}/run_results.json"
        s3.upload_file(run_results_path, bucket_obs, results_key)
        print(f"  ✅ Run results: s3://{bucket_obs}/{results_key}")

        # Print model stats
        print("")
        print("  📈 Model Results:")
        for m in summary["models"]:
            status_icon = "✅" if m["status"] == "success" else "❌"
            rows = f"{m['rows_affected']} rows" if m["rows_affected"] else ""
            print(f"     {status_icon} {m['name']}: {m['execution_time_sec']}s {rows}")
    else:
        print(f"  ⚠️  No run_results.json found at {run_results_path}")

    # Upload manifest.json (model definitions, dependencies)
    manifest_path = f"{dbt_target}/manifest.json"
    if os.path.exists(manifest_path):
        manifest_key = f"{obs_prefix}/manifest.json"
        s3.upload_file(manifest_path, bucket_obs, manifest_key)
        print(f"  ✅ Manifest: s3://{bucket_obs}/{manifest_key}")

    print(f"\n{'='*60}")
    print(
        f"  📍 Observability: http://localhost:9001/browser/{bucket_obs}/{obs_prefix}/"
    )
    print(f"{'='*60}\n")

    return {"run_id": run_id, "success": success_count, "error": error_count}


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
dbt run --threads 1 --select path:models/silver_normalized path:models/gold

echo ""
echo "✅ DBT RUN COMPLETE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        """,
    )

    # Upload observability artifacts
    upload_obs = PythonOperator(
        task_id="upload_observability",
        python_callable=upload_gold_observability,
    )

    dbt_run >> upload_obs
