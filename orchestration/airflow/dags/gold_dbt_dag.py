"""Gold dbt DAG - builds Gold analytical models from Silver using dbt."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


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
- `gold.pace_summary_by_round`
- `gold.signal_quality_rounds`
- `gold.device_health_errors`

**Output:**
- Gold Iceberg tables in: http://localhost:9001/browser/tm-lakehouse-serve/warehouse/gold/
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
cd /opt/tagmarshal/transform/dbt_project

# Set profiles directory
export DBT_PROFILES_DIR=/opt/tagmarshal/transform/dbt_project

echo "  ⏳ Installing dbt dependencies..."
dbt deps --quiet 2>/dev/null || dbt deps

echo ""
echo "  ⏳ Running Gold models..."
dbt run --select path:models/gold

echo ""
echo "✅ GOLD MODELS COMPLETE"
echo ""
echo "  📍 Data Locations:"
echo "     Gold tables: http://localhost:9001/browser/tm-lakehouse-serve/warehouse/gold/"
echo ""
echo "  Models built:"
echo "     • gold.pace_summary_by_round"
echo "     • gold.signal_quality_rounds"
echo "     • gold.device_health_errors"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        """,
    )
