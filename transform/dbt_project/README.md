## dbt project

This dbt project will do two things:
- Build **Gold** models from Silver
- Run **data quality** gates (dbt tests) after Silver writes

Locally we’ll run dbt against Trino; on AWS we’ll run against Athena.


