"""Helpers to display dbt model SQL (silver -> gold provenance) in the dashboard."""

from __future__ import annotations

from pathlib import Path
import streamlit as st


_REPO_ROOT = Path(__file__).resolve().parents[2]
_DBT_GOLD_MODELS_DIR = _REPO_ROOT / "pipeline" / "gold" / "models" / "gold"


@st.cache_data(ttl=300)
def load_dbt_model_sql(filename: str) -> str:
    """Load a dbt model SQL file from pipeline/gold/models/gold/.

    Returns a helpful message if the file cannot be found/read.
    """
    path = _DBT_GOLD_MODELS_DIR / filename
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return f"-- Could not find dbt model file: {path}\n"
    except Exception as e:
        return f"-- Failed to read dbt model file: {path}\n-- {e}\n"


def render_dbt_models_section(
    model_filenames: list[str],
    *,
    title: str = "Gold layer SQL (dbt models built from silver)",
) -> None:
    """Render a consistent provenance section for a list of dbt model files."""
    if not model_filenames:
        return

    st.subheader(title)
    st.caption("These are the dbt model definitions used to build the gold tables from silver.")

    for filename in model_filenames:
        with st.expander(f"dbt model: {filename}", expanded=False):
            st.code(load_dbt_model_sql(filename), language="sql")


