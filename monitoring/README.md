# Monitoring & Alerting

This directory contains monitoring configurations, alerting rules, and observability configs.

## Structure

- **`alerts/`** - Alert definitions and thresholds
- **`dashboards/`** - Monitoring dashboard configurations

## Purpose

Centralize monitoring configuration for:
- **Pipeline Health** - Track DAG runs, failures, durations
- **Data Quality** - Monitor data quality metrics over time
- **System Health** - Track resource usage, errors, performance

## Alert Types

### Pipeline Alerts
- DAG failures
- Long-running tasks
- Data freshness issues

### Data Quality Alerts
- Null rate exceeds threshold
- Duplicate data detected
- Data volume anomalies

### System Alerts
- High resource usage
- Service unavailability
- Storage capacity warnings

## Configuration Format

Alerts are defined in YAML format:

```yaml
alerts:
  - name: "High Null Rate"
    condition: "null_rate > 0.1"
    severity: "warning"
    notify: ["data-team@example.com"]
```

## Integration

Monitoring configs can be used by:
- **Airflow** - Alert on DAG failures
- **dbt** - Alert on test failures
- **Custom scripts** - Monitor data quality metrics
- **Grafana/Prometheus** - Dashboard configurations

## Adding Monitoring

1. Define alert in `alerts/` directory
2. Set appropriate thresholds
3. Configure notification channels
4. Document alert purpose and response procedure

