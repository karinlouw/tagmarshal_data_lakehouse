"""Database utilities for connecting to Trino and executing queries."""

import os
import streamlit as st
import pandas as pd
from trino.dbapi import connect


def get_trino_connection():
    """Create a Trino connection.

    Connection parameters can be overridden via environment variables:
    - TRINO_HOST (default: localhost)
    - TRINO_PORT (default: 8081)
    - TRINO_USER (default: streamlit)
    """
    host = os.environ.get("TRINO_HOST", "localhost")
    port = int(os.environ.get("TRINO_PORT", "8081"))
    user = os.environ.get("TRINO_USER", "streamlit")

    return connect(
        host=host,
        port=port,
        user=user,
        catalog="iceberg",
        schema="gold",
    )


@st.cache_data(ttl=300)  # Cache for 5 minutes
def execute_query(query: str) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame.

    Results are cached for 5 minutes to improve performance.

    Args:
        query: SQL query to execute

    Returns:
        DataFrame with query results
    """
    conn = get_trino_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def test_connection() -> bool:
    """Test if Trino connection is working.

    Returns:
        True if connection is successful, False otherwise
    """
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
        conn.close()
        return True
    except Exception:
        # Let the caller decide how to surface the error in the UI.
        return False
