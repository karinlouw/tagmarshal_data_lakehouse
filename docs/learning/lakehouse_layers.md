## Lakehouse layers (Bronze / Silver / Gold)

### Bronze (raw)
- **What**: original files, minimally validated
- **Why**: lineage, replay, auditing, debugging
- **How to think**: don’t optimize for querying here; optimize for traceability and idempotency

### Silver (conformed)
- **What**: cleaned + normalized tables with a stable schema
- **Why**: the main analytics/query surface (Athena/Trino prune partitions here)
- **How to think**: this is where you enforce consistent types and keys

### Gold (business-ready)
- **What**: metrics and aggregates for BI/product analytics
- **Why**: faster queries + consistent business logic
- **How to think**: build this in dbt so it’s documented, tested, and reviewable


