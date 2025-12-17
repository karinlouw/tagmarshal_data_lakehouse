## How to debug runs (practical checklist)

### 1) Start at the orchestrator
- Open Airflow UI and find the failed DAG run
- Click the failed task and open logs

### 2) Identify which stage failed
- Bronze ingest (upload/validation)
- Silver ETL (Spark)
- Data quality gate (dbt tests)
- Gold build (dbt models)

### 3) Look for run identifiers
We’ll log consistent fields like `run_id`, `course_id`, and `ingest_date`. Use those to find:
- the run summary JSON in MinIO/S3 (observability bucket)
- quarantine report objects (quarantine bucket)

### 4) Common failure types
- Missing columns or unexpected headers in CSV
- Timestamp parse issues (`locations[i].startTime`)
- Invalid coordinates / swapped lat/lon
- Duplicate storms (dedup not applied or bad keys)


