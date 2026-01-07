"""Configuration loader for Tagmarshal lakehouse - reads env vars and provides TMConfig dataclass."""

from __future__ import annotations

import os
from dataclasses import dataclass


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _env(name: str, default: str | None = None) -> str:
    """Get required env var, raise if missing (fail-fast approach).

    Why: Better to fail immediately than silently use wrong config.
    """
    v = os.getenv(name, default)
    if v is None or v == "":
        raise ValueError(f"Missing required env var: {name}")
    return v


def _env_bool(name: str, default: bool = False) -> bool:
    """Get boolean env var (accepts 1/true/yes/y/on).

    Why: Environment variables are strings, so we normalize common boolean formats.
    """
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


# =============================================================================
# CONFIGURATION DATACLASS
# =============================================================================


@dataclass(frozen=True)
class TMConfig:
    """Immutable config object holding all lakehouse settings from env vars.

    Why frozen=True: Prevents accidental config changes after creation.
    Why dataclass: Type-safe, clean, and easy to test.
    """

    # Environment
    env: str  # "local" or "aws"

    # S3/MinIO buckets (medallion architecture layers)
    bucket_landing: str  # Bronze: raw data landing zone
    bucket_source: str  # Silver: cleaned, conformed source of truth
    bucket_serve: str  # Gold: analytics-ready data
    bucket_quarantine: str  # Invalid data (for debugging)
    bucket_observability: str  # Run logs and artifacts

    # S3 connection settings
    s3_endpoint: str | None  # http://minio:9000 (local) or None (AWS)
    s3_region: str  # AWS region (us-east-1)
    s3_access_key: str | None  # MinIO/AWS access key
    s3_secret_key: str | None  # MinIO/AWS secret key
    s3_force_path_style: bool  # True for MinIO, False for AWS S3

    # Iceberg catalog settings
    iceberg_catalog_type: str  # "rest" (local) or "glue" (AWS)
    iceberg_rest_uri: str | None  # http://iceberg-rest:8181 (local only)
    iceberg_warehouse_silver: str  # s3://bucket/warehouse (where Silver tables live)
    iceberg_warehouse_gold: str  # s3://bucket/warehouse (where Gold tables live)

    # Iceberg schema names (namespaces)
    db_silver: str  # "silver" schema name
    db_gold: str  # "gold" schema name

    # Observability
    obs_prefix: str  # Optional prefix for observability bucket paths

    # Local file input
    local_input_dir: str  # /opt/tagmarshal/input (where CSV/JSON files are mounted)

    # Coordinate sanity bounds (used by Silver ETL for quarantine logic)
    coord_min_lon: float
    coord_max_lon: float
    coord_min_lat: float
    coord_max_lat: float

    # API settings (for future TagMarshal API integration)
    data_source: str  # "file" (local CSV/JSON) or "api" (TagMarshal API)
    api_base_url: str | None
    api_key: str | None
    api_timeout: int

    @staticmethod
    def from_env() -> "TMConfig":
        """Build TMConfig from environment variables.

        Usage: cfg = TMConfig.from_env()

        Why staticmethod: Can call without creating an instance first.
        """
        return TMConfig(
            # Environment
            env=_env("TM_ENV", "local"),
            # Buckets (required - no defaults)
            bucket_landing=_env("TM_BUCKET_LANDING"),
            bucket_source=_env("TM_BUCKET_SOURCE"),
            bucket_serve=_env("TM_BUCKET_SERVE"),
            bucket_quarantine=_env("TM_BUCKET_QUARANTINE"),
            bucket_observability=_env("TM_BUCKET_OBSERVABILITY"),
            # S3 settings (optional for AWS - uses IAM roles)
            s3_endpoint=os.getenv("TM_S3_ENDPOINT"),  # None = AWS, set = MinIO
            s3_region=_env("TM_S3_REGION", "us-east-1"),
            s3_access_key=os.getenv("TM_S3_ACCESS_KEY"),  # Optional (IAM in AWS)
            s3_secret_key=os.getenv("TM_S3_SECRET_KEY"),  # Optional (IAM in AWS)
            s3_force_path_style=_env_bool(
                "TM_S3_FORCE_PATH_STYLE", True
            ),  # MinIO needs this
            # Iceberg catalog
            iceberg_catalog_type=_env("TM_ICEBERG_CATALOG_TYPE", "rest"),
            iceberg_rest_uri=os.getenv("TM_ICEBERG_REST_URI"),  # Only for "rest" type
            iceberg_warehouse_silver=_env("TM_ICEBERG_WAREHOUSE_SILVER"),
            iceberg_warehouse_gold=_env("TM_ICEBERG_WAREHOUSE_GOLD"),
            # Schema names
            db_silver=_env("TM_DB_SILVER", "silver"),
            db_gold=_env("TM_DB_GOLD", "gold"),
            # Observability
            obs_prefix=os.getenv("TM_OBS_PREFIX", ""),  # Optional prefix
            # Local input
            local_input_dir=_env("TM_LOCAL_INPUT_DIR", "/opt/tagmarshal/input"),
            # Coordinate sanity bounds (centralized defaults)
            coord_min_lon=float(os.getenv("TM_COORD_MIN_LON", "-180")),
            coord_max_lon=float(os.getenv("TM_COORD_MAX_LON", "180")),
            coord_min_lat=float(os.getenv("TM_COORD_MIN_LAT", "-90")),
            coord_max_lat=float(os.getenv("TM_COORD_MAX_LAT", "90")),
            # API settings
            data_source=_env("TM_DATA_SOURCE", "file"),  # Default to file-based
            api_base_url=os.getenv("TM_API_BASE_URL"),
            api_key=os.getenv("TM_API_KEY"),
            api_timeout=int(os.getenv("TM_API_TIMEOUT", "30")),
        )
