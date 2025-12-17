## Spark jobs (Bronze → Silver)

This folder will contain PySpark jobs run by Airflow.

Key expectations:
- Read config from env (so local ↔ AWS is config-only).
- Structured logs (JSON-like) for observability.
- Fail fast on schema/quality issues; write quarantine reports.


