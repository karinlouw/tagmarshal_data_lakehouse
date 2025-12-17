## Iceberg basics (for juniors)

### What Iceberg is
Apache Iceberg is a table format for data lakes. Instead of “a folder of Parquet files”, you get a real table with:
- snapshots (time travel)
- schema evolution
- partition evolution
- atomic commits

### Catalog vs warehouse
- **Catalog**: stores table metadata references (e.g. REST catalog locally, Glue catalog on AWS)
- **Warehouse**: where the data + metadata files live (typically S3/MinIO)

### Partitions (important for performance)
Partitions help engines skip reading unnecessary files. In this project:
- **Bronze**: partition by `course_id` + `ingest_date`
- **Silver**: partition by `course_id` + `event_date` derived from fix timestamps


