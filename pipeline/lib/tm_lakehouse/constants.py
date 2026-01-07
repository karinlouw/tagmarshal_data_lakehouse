"""Shared constants for the TagMarshal lakehouse pipeline.

This module centralizes constants that are used across multiple scripts,
avoiding hardcoded values scattered throughout the codebase.
"""

import os
from typing import List

# =============================================================================
# SPARK JAR PATHS
# =============================================================================
# These JARs are required for Spark to work with Iceberg and S3/MinIO.
# The paths assume the standard Docker container layout.

SPARK_EXTRA_JARS_DIR = "/opt/spark/extra-jars"

# Individual JAR files
ICEBERG_SPARK_JAR = f"{SPARK_EXTRA_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"
AWS_SDK_BUNDLE_JAR = f"{SPARK_EXTRA_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar"
HADOOP_AWS_JAR = f"{SPARK_EXTRA_JARS_DIR}/hadoop-aws-3.3.4.jar"
AWS_SDK_V2_BUNDLE_JAR = f"{SPARK_EXTRA_JARS_DIR}/bundle-2.20.18.jar"
AWS_URL_CONNECTION_JAR = f"{SPARK_EXTRA_JARS_DIR}/url-connection-client-2.20.18.jar"

# All JARs in order of importance
ALL_SPARK_JARS = [
    ICEBERG_SPARK_JAR,
    AWS_SDK_BUNDLE_JAR,
    HADOOP_AWS_JAR,
    AWS_SDK_V2_BUNDLE_JAR,
    AWS_URL_CONNECTION_JAR,
]


def get_existing_spark_jars() -> List[str]:
    """Return list of JAR paths that exist on the filesystem.

    This allows the pipeline to work even if some optional JARs are missing.
    """
    return [jar for jar in ALL_SPARK_JARS if os.path.exists(jar)]


def get_spark_jars_config() -> str:
    """Return comma-separated JAR paths for Spark config.

    Usage:
        spark_builder.config("spark.jars", get_spark_jars_config())
    """
    return ",".join(get_existing_spark_jars())


# =============================================================================
# COURSE TOPOLOGY CONSTANTS
# =============================================================================
# These define the standard section boundaries for different course types.
# Based on analysis of raw data from 5 courses.

# Number of sections per nine (approximate, varies slightly by course)
SECTIONS_PER_NINE = 27

# Hole number threshold - if max hole >= this, it's an 18-hole course
# (holes don't reset, they go 1-18)
EIGHTEEN_HOLE_THRESHOLD = 10

# Section number thresholds for nine boundaries
# (for courses where holes reset 1-9 per nine)
NINE_1_MAX_SECTION = 27
NINE_2_MAX_SECTION = 54
NINE_3_MAX_SECTION = 81


# =============================================================================
# COORDINATE VALIDATION BOUNDS
# =============================================================================
# These are sanity bounds for GPS coordinates.
# Coordinates outside these bounds are quarantined as invalid.

COORD_BOUNDS = {
    "lon_min": -180.0,
    "lon_max": 180.0,
    "lat_min": -90.0,
    "lat_max": 90.0,
}


# =============================================================================
# FILE FORMAT DETECTION
# =============================================================================

SUPPORTED_FILE_FORMATS = ["csv", "json"]
DEFAULT_FILE_FORMAT = "csv"


# =============================================================================
# SPARK CONFIGURATION DEFAULTS
# =============================================================================
# These are fallback defaults if not specified in environment.

SPARK_DEFAULTS = {
    "master": "local[2]",
    "driver_memory": "1g",
    "executor_memory": "1g",
    "shuffle_partitions": 8,
}
