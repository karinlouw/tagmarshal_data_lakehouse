# Test Quick Start Guide

A super simple guide to get you started with our tests.

## What Are Tests?

Tests check if code works correctly. They're like quality checks that run automatically.

## Running Tests

```bash
# Run all tests
pytest pipeline/tests/

# Run one test file
pytest pipeline/tests/unit/bronze/test_ingest.py

# Run one specific test
pytest pipeline/tests/unit/bronze/test_ingest.py::test_count_csv_rows
```

## Understanding Test Results

- ✅ `PASSED` = Test worked correctly
- ❌ `FAILED` = Test found a problem
- 💥 `ERROR` = Something broke (missing file, import error, etc.)

## Simple Test Example

```python
def test_count_csv_rows(sample_csv_path):
    """Count rows in CSV file."""
    count = count_csv_rows(sample_csv_path)
    assert count == 2  # Should be 2 rows
```

**What this does:**
1. Calls `count_csv_rows()` with a test CSV file
2. Checks if the result is 2
3. Passes if it's 2, fails if it's not

## Key Words to Know

- **`assert`** = "Check if this is true"
- **`fixture`** = Reusable test data (like `sample_csv_path`)
- **`mock`** = Fake version of something (like fake S3 instead of real S3)
- **`patch`** = Temporarily replace a function with a mock

## Reading a Test

1. **Function name** tells you what's being tested
2. **Docstring** explains the purpose
3. **Code** does the testing
4. **`assert`** statements check if things are correct

## Example: Breaking Down a Test

```python
def test_upload_file_to_bronze_csv(sample_csv_path, sample_config, mock_s3_client):
    """Test that CSV upload works."""
    # Replace real S3 with fake S3
    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        # Make it think file doesn't exist yet
        with patch("bronze.ingest.object_exists", return_value=False):
            # Try to upload
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )
            
            # Check results
            assert result.row_count == 2  # Should have 2 rows
            assert result.skipped is False  # Should not be skipped
```

**In plain English:**
- Use fake S3 (not real one)
- Try uploading a CSV file
- Check that it says there are 2 rows
- Check that it wasn't skipped

## Common Commands

```bash
# See what tests exist
pytest pipeline/tests/ --collect-only

# Run tests and see output
pytest pipeline/tests/ -v -s

# Run tests and stop at first failure
pytest pipeline/tests/ -x

# Run tests and show coverage
pytest pipeline/tests/ --cov=pipeline
```

## Where to Start

1. **Read** `HOW_TESTS_WORK.md` for detailed explanation
2. **Run** a simple test: `pytest pipeline/tests/unit/bronze/test_ingest.py::test_count_csv_rows`
3. **Look at** the test file to see how it works
4. **Modify** a test to see what happens

## Need Help?

- Check the error message - it usually tells you what's wrong
- Look at similar tests for examples
- Read `HOW_TESTS_WORK.md` for more details

