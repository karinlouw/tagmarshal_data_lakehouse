# Tests

This directory contains unit tests, integration tests, data quality tests, and test fixtures.

## Structure

- **`unit/`** - Unit tests for all pipeline components
  - **`bronze/`** - Bronze layer ingestion tests
  - **`silver/`** - Silver ETL transformation tests
  - **`lib/`** - Library module tests (config, s3, registry, observability, api)
  - **`orchestration/`** - Airflow DAG task tests
  - **`scripts/`** - Utility script tests
- **`integration/`** - End-to-end pipeline tests
- **`data_quality/`** - Data quality validation tests
- **`fixtures/`** - Test data files and sample data

## Test Types

### Unit Tests
Comprehensive unit tests for all pipeline components with mocked external dependencies:
- **Bronze Layer**: File format detection, validation, upload, idempotency
- **Silver Layer**: Location index discovery, transformation logic, coordinate validation
- **Library Modules**: Config parsing, S3 operations, registry tracking, observability, API client
- **Orchestration**: DAG task functions with mocked Airflow context
- **Scripts**: Backfill orchestration and job tracking

All unit tests use mocking for external dependencies (S3, Postgres, Spark) to ensure fast, isolated testing.

### Integration Tests
Test the full pipeline from Bronze → Silver → Gold:
- Verify data flows correctly through all layers
- Check that transformations produce expected results
- Validate idempotency (re-running produces same results)

### Data Quality Tests
Validate data quality rules:
- Check for duplicates
- Verify null rates are within acceptable thresholds
- Validate data types and ranges
- Check referential integrity

### dbt Tests
dbt-specific tests are in `gold/tests/`:
- `duplicate_round_fix_timestamp.sql` - Checks for duplicate keys

## Running Tests

```bash
# Install test dependencies
pip install -r pipeline/tests/requirements.txt

# Run all unit tests
pytest pipeline/tests/unit/

# Run specific test file
pytest pipeline/tests/unit/bronze/test_ingest.py

# Run with coverage
pytest pipeline/tests/ --cov=pipeline --cov-report=html

# Run integration tests
pytest pipeline/tests/integration/

# Run data quality tests
pytest pipeline/tests/data_quality/

# Run all tests
pytest pipeline/tests/
```

## Test Fixtures

Test fixtures are located in `fixtures/`:
- `sample_csv.csv` - Valid CSV with multiple location indices
- `sample_json.json` - Valid JSON with nested locations
- `sample_json_mongodb.json` - MongoDB format with `$oid` and `$date`
- `invalid_data/` - Invalid test data for error handling tests

## Documentation

- **`HOW_TESTS_WORK.md`** - Comprehensive guide explaining how tests work (great for beginners!)
- **`QUICK_START.md`** - Quick reference for running and understanding tests
- **`TEST_SUMMARY.md`** - Summary of test coverage and results

## Adding Tests

1. Create test file in appropriate subdirectory
2. Follow pytest conventions
3. Use fixtures from `tests/fixtures/` for test data
4. Document what the test validates

**New to testing?** Start with `HOW_TESTS_WORK.md` - it explains everything step by step!

