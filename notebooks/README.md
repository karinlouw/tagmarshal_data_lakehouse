# Jupyter Notebooks

This directory contains Jupyter notebooks for data exploration and analysis.

## Structure

- **`exploration/`** - Data exploration notebooks
- **`analysis/`** - Analysis and reporting notebooks

## Usage

### Starting Jupyter

```bash
# Install Jupyter (if not already installed)
pip install jupyter

# Start Jupyter server
jupyter notebook notebooks/
```

### Best Practices

- **Clear naming**: Use descriptive names like `data_quality_analysis.ipynb`
- **Documentation**: Add markdown cells explaining what you're doing
- **Clean outputs**: Clear outputs before committing to git
- **Version control**: Use `nbstripout` to strip outputs from notebooks in git

## Notebook Guidelines

1. **Start with purpose**: First cell should explain what the notebook does
2. **Load data**: Use Trino connection to load data from Iceberg tables
3. **Document findings**: Use markdown cells to document insights
4. **Export results**: Save important visualizations or results

## Example Connection

```python
from trino.dbapi import connect
from trino.auth import BasicAuthentication

conn = connect(
    host="localhost",
    port=8081,
    user="admin",
    auth=BasicAuthentication("admin", "admin"),
    catalog="iceberg",
    schema="silver"
)

# Query data
import pandas as pd
df = pd.read_sql("SELECT * FROM fact_telemetry_event LIMIT 100", conn)
```

