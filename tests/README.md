# Tests

This directory contains integration tests, data quality tests, and test fixtures.

## Structure

- **`integration/`** - End-to-end pipeline tests
- **`data_quality/`** - Data quality validation tests
- **`fixtures/`** - Test data files and sample data

## Test Types

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
dbt-specific tests are in `transform/dbt_project/tests/`:
- `duplicate_round_fix_timestamp.sql` - Checks for duplicate keys

## Running Tests

```bash
# Run integration tests
pytest tests/integration/

# Run data quality tests
pytest tests/data_quality/

# Run all tests
pytest tests/
```

## Adding Tests

1. Create test file in appropriate subdirectory
2. Follow pytest conventions
3. Use fixtures from `tests/fixtures/` for test data
4. Document what the test validates

