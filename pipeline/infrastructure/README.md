# Infrastructure

This directory contains infrastructure configuration files for the data lakehouse.

## Structure

- **`database/`** - Database migration scripts
  - SQL migrations for creating tables (e.g., ingestion_log)
  
- **`services/`** - Docker service configurations
  - Service-specific configs (Trino, etc.)
  - These are mounted into Docker containers at runtime

## Database Migrations

Migrations are SQL scripts that create or modify database schemas. They're numbered sequentially (e.g., `001_create_ingestion_log.sql`).

**When to add a migration:**
- Creating a new table
- Adding a new column
- Modifying an existing schema

## Service Configs

Service configurations are mounted into Docker containers. For example:
- `services/trino/` - Trino configuration files (catalog, config, JVM settings)

**Note:** These are read-only mounts - the containers read these configs but don't modify them.

