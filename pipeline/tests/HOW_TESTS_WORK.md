# How Our Tests Work - A Beginner's Guide

This guide explains how our test suite works so you can understand, run, and write tests.

## What Are Tests?

Tests are code that checks if other code works correctly. Think of them as automated quality checks:
- **Unit tests** check individual functions in isolation
- **Integration tests** check how different parts work together

## Test Structure

```
pipeline/tests/
├── conftest.py              # Shared test setup (fixtures)
├── unit/                    # Unit tests (test individual functions)
│   ├── bronze/              # Tests for bronze layer
│   ├── silver/              # Tests for silver layer
│   └── lib/                 # Tests for shared utilities
├── integration/             # Integration tests (test full workflows)
└── fixtures/               # Sample data files for testing
```

## Key Concepts

### 1. Test Functions

Every test is a function that starts with `test_`. Pytest automatically finds and runs these.

```python
def test_upload_file_to_bronze_csv():
    """Test that CSV upload works correctly."""
    # Test code here
    assert result.row_count > 0  # Check that rows were counted
```

**What happens:**
- Pytest finds all `test_*.py` files
- Runs all functions starting with `test_`
- Reports which passed (`PASSED`) or failed (`FAILED`)

### 2. Assertions

Assertions check if something is true. If false, the test fails.

```python
assert result.row_count == 2  # Must be exactly 2
assert result.skipped is False  # Must be False
assert "course_id=" in result.key  # String must contain this
```

**Common assertions:**
- `assert x == y` - Values must be equal
- `assert x is True` - Must be True (not just truthy)
- `assert "text" in string` - String must contain text
- `assert x > 0` - Value must be greater than 0

### 3. Fixtures

Fixtures are reusable test data or setup. They're defined in `conftest.py` or test files.

```python
@pytest.fixture
def sample_config():
    """Create a test configuration."""
    return TMConfig(
        env="local",
        bucket_landing="tm-lakehouse-landing-zone",
        # ... more config
    )
```

**How to use:**
```python
def test_something(sample_config):  # Pass fixture as parameter
    result = upload_file(sample_config, ...)
    assert result.bucket == "tm-lakehouse-landing-zone"
```

**Why fixtures?**
- Reuse the same test data across many tests
- Keep tests consistent
- Easy to update test data in one place

### 4. Mocking

Mocking replaces real dependencies (like S3, databases) with fake ones that we control.

**Why mock?**
- Real S3/databases are slow and need credentials
- We want fast, isolated tests
- We can simulate errors easily

**Example:**
```python
# Instead of real S3, use a mock
mock_s3_client = MagicMock()
mock_s3_client.upload_file = MagicMock()  # Fake upload function

# Tell our code to use the mock
with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
    result = upload_file_to_bronze(...)  # Uses mock, not real S3
    
# Check that upload was called
mock_s3_client.upload_file.assert_called_once()
```

**Key mocking concepts:**
- `MagicMock()` - Creates a fake object
- `patch()` - Temporarily replaces a function/class
- `return_value` - What the mock should return
- `assert_called_once()` - Verify the mock was called

## Example: Understanding a Real Test

Let's break down a real test from our suite:

```python
def test_upload_file_to_bronze_csv(sample_csv_path, sample_config, mock_s3_client):
    """Test upload_file_to_bronze() uploads CSV successfully."""
    # Step 1: Replace real S3 with our mock
    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        # Step 2: Make object_exists return False (file doesn't exist yet)
        with patch("bronze.ingest.object_exists", return_value=False):
            # Step 3: Call the actual function we're testing
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )
            
            # Step 4: Check the results
            assert result.bucket == "tm-lakehouse-landing-zone"
            assert result.row_count == 2
            assert result.header_ok is True
            assert result.skipped is False
            # Step 5: Verify S3 upload was called (but with mock, not real S3)
            mock_s3_client.upload_file.assert_called_once()
```

**What this test does:**
1. Uses fixtures: `sample_csv_path` (test CSV file), `sample_config` (test config)
2. Mocks S3: Replaces real S3 with `mock_s3_client`
3. Calls the function: Tests `upload_file_to_bronze()`
4. Checks results: Verifies the function returned correct values
5. Verifies behavior: Confirms S3 upload was attempted

## Test Types in Our Suite

### Unit Tests

Test individual functions in isolation.

**Example:** `test_count_csv_rows()`
```python
def test_count_csv_rows(sample_csv_path):
    """Test count_csv_rows() counts data rows (excludes header)."""
    count = count_csv_rows(sample_csv_path)
    assert count == 2  # Should be 2 data rows
```

**What it tests:**
- Just the `count_csv_rows()` function
- Doesn't test S3 upload or anything else
- Fast and focused

### Integration Tests

Test how multiple parts work together.

**Example:** `test_bronze_upload_csv_row_count_matches_actual()`
```python
def test_bronze_upload_csv_row_count_matches_actual(sample_csv_path, sample_config):
    """Test that Bronze upload reports correct row count for CSV."""
    mock_s3 = MagicMock()
    
    with patch("bronze.ingest.make_s3_client", return_value=mock_s3):
        with patch("bronze.ingest.object_exists", return_value=False):
            result = upload_file_to_bronze(...)
            
            # Verify row count matches actual data rows
            assert result.row_count > 0
```

**What it tests:**
- Full upload process (validation + upload)
- Multiple functions working together
- Real-world scenario

### Data Quality Tests

Test that data meets quality requirements.

**Example:** `test_coordinates_in_valid_range()`
```python
def test_coordinates_in_valid_range(sample_json_path):
    """Test that coordinates are in valid ranges."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    for round_data in data:
        for location in round_data.get("locations", []):
            coords = location.get("fixCoordinates", [])
            if len(coords) >= 2:
                longitude, latitude = coords[0], coords[1]
                if longitude is not None:
                    assert -180 <= longitude <= 180  # Valid range
                if latitude is not None:
                    assert -90 <= latitude <= 90  # Valid range
```

**What it tests:**
- Data quality rules
- Value ranges
- Data structure

## Running Tests

### Run All Tests
```bash
pytest pipeline/tests/
```

### Run Specific Test File
```bash
pytest pipeline/tests/unit/bronze/test_ingest.py
```

### Run Specific Test
```bash
pytest pipeline/tests/unit/bronze/test_ingest.py::test_count_csv_rows
```

### Run with Verbose Output
```bash
pytest pipeline/tests/ -v
```

### Run and See Print Statements
```bash
pytest pipeline/tests/ -v -s
```

## Understanding Test Output

### Passing Test
```
pipeline/tests/unit/bronze/test_ingest.py::test_count_csv_rows PASSED
```
✅ Test passed - everything worked as expected

### Failing Test
```
pipeline/tests/unit/bronze/test_ingest.py::test_count_csv_rows FAILED
AssertionError: assert 3 == 2
```
❌ Test failed - the assertion `assert count == 2` failed because count was 3

### Error in Test
```
pipeline/tests/unit/bronze/test_ingest.py::test_count_csv_rows ERROR
FileNotFoundError: [Errno 2] No such file or directory: 'sample.csv'
```
💥 Test error - something went wrong (missing file, import error, etc.)

## Common Patterns

### Pattern 1: Testing with Mocks
```python
def test_something(mock_s3_client):
    with patch("module.function", return_value=mock_s3_client):
        result = do_something()
        assert result is not None
```

### Pattern 2: Testing Error Cases
```python
def test_error_case():
    with pytest.raises(ValueError, match="missing required"):
        function_that_should_fail()
```

### Pattern 3: Testing with Test Data
```python
def test_with_data(sample_csv_path):
    result = process_file(sample_csv_path)
    assert result.row_count > 0
```

### Pattern 4: Testing Multiple Scenarios
```python
def test_multiple_formats(tmp_path):
    # Test CSV
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("header\nrow1")
    assert detect_format(str(csv_file)) == "csv"
    
    # Test JSON
    json_file = tmp_path / "test.json"
    json_file.write_text('{"key": "value"}')
    assert detect_format(str(json_file)) == "json"
```

## Writing Your First Test

Here's a simple example to get started:

```python
def test_my_function():
    """Test that my_function returns the expected value."""
    # Arrange: Set up test data
    input_value = 5
    
    # Act: Call the function
    result = my_function(input_value)
    
    # Assert: Check the result
    assert result == 10  # Expected value
```

**The AAA Pattern:**
- **Arrange**: Set up test data
- **Act**: Call the function being tested
- **Assert**: Check the results

## Tips for Reading Tests

1. **Start with the test name** - It describes what's being tested
2. **Read the docstring** - Explains the test's purpose
3. **Follow the flow** - See what fixtures are used, what's mocked, what's called
4. **Check the assertions** - These are what the test is verifying
5. **Look at fixtures** - Understand what test data is being used

## Common Questions

**Q: Why do we mock S3 instead of using real S3?**
A: Real S3 needs credentials, network access, and costs money. Mocks are fast, free, and work offline.

**Q: What if a test fails?**
A: Read the error message - it tells you what went wrong. Check:
- What assertion failed?
- What was the expected vs actual value?
- Is there a missing dependency?

**Q: How do I know what to test?**
A: Test:
- Happy path (normal usage)
- Error cases (missing data, invalid input)
- Edge cases (empty files, boundary values)

**Q: Can I run tests while developing?**
A: Yes! Run tests frequently to catch bugs early:
```bash
pytest pipeline/tests/unit/bronze/test_ingest.py -v
```

## Next Steps

1. **Run the tests** to see them in action
2. **Read a simple test** like `test_count_csv_rows()` 
3. **Modify a test** to see what happens
4. **Write a new test** for a function you understand

## Resources

- **Pytest docs**: https://docs.pytest.org/
- **Python unittest.mock**: https://docs.python.org/3/library/unittest.mock.html
- **Our test files**: Look at `pipeline/tests/unit/bronze/test_ingest.py` for examples

Happy testing! 🧪

