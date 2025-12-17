## dbt testing 101

### Why dbt for data quality
dbt tests are simple, reviewable SQL checks that run inside your warehouse/query engine (Trino locally, Athena in AWS).

### The core test types you’ll use
- **not_null**: required fields must be present
- **accepted_range**: numeric sanity (lat/lon bounds)
- **unique / unique combinations**: dedup guarantees
- **custom SQL tests**: business rules (duplicate rate thresholds, swapped lat/lon heuristics)

### “Gating”
We run `dbt test` immediately after writing Silver. If tests fail, the Airflow DAG fails, so the issue is visible and traceable.


