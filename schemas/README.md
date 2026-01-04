# Table Schemas

This directory contains table schema definitions and DDL scripts for documentation and reference.

## Structure

- **`bronze/`** - Bronze layer table schemas (raw/landing zone)
- **`silver/`** - Silver layer table schemas (cleaned/transformed)
- **`gold/`** - Gold layer table schemas (analytics-ready)

## Purpose

These schemas serve as:
- **Documentation** - Reference for table structures
- **DDL Scripts** - SQL scripts for creating tables (if needed)
- **Schema Evolution** - Track changes to table structures over time

## Note

In this project, schemas are primarily managed by:
- **Iceberg** - Handles schema evolution automatically
- **dbt** - Defines Gold layer schemas in `transform/dbt_project/models/`
- **Spark ETL** - Defines Silver layer schema in `jobs/spark/silver_etl.py`

These schema files are for **reference and documentation** purposes.

