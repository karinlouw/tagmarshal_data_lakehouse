# Data Validations

This directory contains data validation rules, quality thresholds, and business rules.

## Structure

- **`rules/`** - Validation rule definitions (YAML/JSON)
- **`thresholds/`** - Quality thresholds and acceptable limits

## Purpose

Centralize validation logic to:
- Make rules configurable (not hardcoded)
- Enable rule versioning and auditing
- Share rules across different tools (dbt, Spark, Python)

## Example Validation Rules

### Pace Validation
```yaml
pace:
  min: 0
  max: 3600  # 1 hour behind goal
  null_allowed: false
  description: "Pace must be between 0 and 3600 seconds"
```

### Battery Validation
```yaml
battery_percentage:
  min: 0
  max: 100
  null_allowed: true
  warning_threshold: 20
  description: "Battery percentage should be 0-100, warn if < 20%"
```

## Usage

Validation rules can be loaded by:
- **dbt tests** - Reference thresholds in dbt test definitions
- **Spark ETL** - Validate data during transformation
- **Python scripts** - Load and apply rules programmatically

## Adding Rules

1. Create YAML file in `rules/` directory
2. Define validation criteria clearly
3. Document business justification
4. Update this README with new rule

