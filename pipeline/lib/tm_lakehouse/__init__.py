"""Shared utilities for TagMarshal lakehouse pipeline.

Modules:
    config - Environment configuration (TMConfig)
    s3 - S3/MinIO client utilities
    api - TagMarshal API client
    registry - Ingestion registry for backfill tracking
    observability - Logging and metrics
"""

from .config import TMConfig
from .s3 import make_s3_client, object_exists

__all__ = ["TMConfig", "make_s3_client", "object_exists"]
