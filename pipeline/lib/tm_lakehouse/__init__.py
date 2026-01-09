"""Shared utilities for TagMarshal lakehouse pipeline.

Modules:
    config - Environment configuration (TMConfig)
    s3 - S3/MinIO client utilities (requires boto3)
    api - TagMarshal API client
    registry - Ingestion registry for backfill tracking
    observability - Logging and metrics
    constants - Shared constants (JAR paths, thresholds)
    spark_utils - Spark session creation utilities
"""

from .config import TMConfig

# Only import s3 utilities if boto3 is available
# (not available in Spark container by default)
try:
    from .s3 import make_s3_client, object_exists

    __all__ = ["TMConfig", "make_s3_client", "object_exists"]
except ImportError:
    __all__ = ["TMConfig"]
