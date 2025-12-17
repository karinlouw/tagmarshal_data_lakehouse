## Airflow 101 (what you need to know)

### Key concepts
- **DAG**: the workflow definition (a graph of tasks)
- **Task**: one unit of work (e.g. upload Bronze, run Spark Silver, run dbt tests)
- **Schedule**: when Airflow runs a DAG automatically (we’ll start manual-first)
- **Run ID**: unique identifier for one execution (used in logging + observability)

### What you’ll do in this project
- Trigger DAGs manually during development
- Read logs in the UI
- Pass parameters like `course_id` and `ingest_date`


