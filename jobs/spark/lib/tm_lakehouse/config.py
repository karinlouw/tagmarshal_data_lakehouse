"""Configuration loader for Tagmarshal lakehouse - reads env vars and provides TMConfig dataclass."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    """Get required env var, raise if missing."""
    v = os.getenv(name, default)
    if v is None or v == "":
        raise ValueError(f"Missing required env var: {name}")
    return v


def _env_bool(name: str, default: bool = False) -> bool:
    """Get boolean env var (accepts 1/true/yes/y/on)."""
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class TMConfig:
    """Immutable config object holding all lakehouse settings from env vars."""

    env: str
    bucket_landing: str  # Bronze: raw data landing zone
    bucket_source: str   # Silver: cleaned, conformed source of truth
    bucket_serve: str    # Gold: analytics-ready data
    bucket_quarantine: str
    bucket_observability: str
    s3_endpoint: str | None
    s3_region: str
    s3_access_key: str | None
    s3_secret_key: str | None
    s3_force_path_style: bool
    iceberg_catalog_type: str  # rest | glue
    iceberg_rest_uri: str | None
    iceberg_warehouse_silver: str
    iceberg_warehouse_gold: str
    db_silver: str
    db_gold: str
    obs_prefix: str
    local_input_dir: str
    # API settings (for future TagMarshal API integration)
    data_source: str  # "file" or "api"
    api_base_url: str | None
    api_key: str | None
    api_timeout: int

    @staticmethod
    def from_env() -> "TMConfig":
        """Build TMConfig from environment variables."""
        return TMConfig(
            env=_env("TM_ENV", "local"),
            bucket_landing=_env("TM_BUCKET_LANDING"),
            bucket_source=_env("TM_BUCKET_SOURCE"),
            bucket_serve=_env("TM_BUCKET_SERVE"),
            bucket_quarantine=_env("TM_BUCKET_QUARANTINE"),
            bucket_observability=_env("TM_BUCKET_OBSERVABILITY"),
            s3_endpoint=os.getenv("TM_S3_ENDPOINT"),
            s3_region=_env("TM_S3_REGION", "us-east-1"),
            s3_access_key=os.getenv("TM_S3_ACCESS_KEY"),
            s3_secret_key=os.getenv("TM_S3_SECRET_KEY"),
            s3_force_path_style=_env_bool("TM_S3_FORCE_PATH_STYLE", True),
            iceberg_catalog_type=_env("TM_ICEBERG_CATALOG_TYPE", "rest"),
            iceberg_rest_uri=os.getenv("TM_ICEBERG_REST_URI"),
            iceberg_warehouse_silver=_env("TM_ICEBERG_WAREHOUSE_SILVER"),
            iceberg_warehouse_gold=_env("TM_ICEBERG_WAREHOUSE_GOLD"),
            db_silver=_env("TM_DB_SILVER", "silver"),
            db_gold=_env("TM_DB_GOLD", "gold"),
            obs_prefix=os.getenv("TM_OBS_PREFIX", ""),
            local_input_dir=_env("TM_LOCAL_INPUT_DIR", "/opt/tagmarshal/input"),
            # API settings
            data_source=_env("TM_DATA_SOURCE", "file"),
            api_base_url=os.getenv("TM_API_BASE_URL"),
            api_key=os.getenv("TM_API_KEY"),
            api_timeout=int(os.getenv("TM_API_TIMEOUT", "30")),
        )
