#!/bin/bash
# Suppress FutureWarning from Airflow's internal configuration code
export PYTHONWARNINGS="ignore::FutureWarning"

# Execute the original command
exec "$@"

