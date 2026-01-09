"""Spark session creation utilities for the TagMarshal lakehouse pipeline.

This module provides a centralized way to create SparkSession objects
configured for Iceberg and S3/MinIO, avoiding duplicated configuration
across multiple scripts.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession

from .config import TMConfig
from .constants import get_spark_jars_config


def create_iceberg_spark_session(
    app_name: str,
    config: Optional[TMConfig] = None,
    master: Optional[str] = None,
    log_level: str = "WARN",
) -> SparkSession:
    """Create a SparkSession configured for Iceberg and S3/MinIO.
    
    This is the standard way to create a Spark session for ETL jobs in
    this lakehouse. It handles all the Iceberg catalog configuration,
    S3 credentials, and JAR dependencies.
    
    Args:
        app_name: Name for the Spark application (shown in Spark UI)
        config: TMConfig instance. If None, loads from environment.
        master: Spark master URL. If None, uses config or "local[2]"
        log_level: Spark log level (ERROR, WARN, INFO, DEBUG)
    
    Returns:
        Configured SparkSession
    
    Example:
        spark = create_iceberg_spark_session(
            app_name="silver_etl_bradshawfarmgc_2024-01-15",
            config=TMConfig.from_env()
        )
    """
    if config is None:
        config = TMConfig.from_env()
    
    # Determine master URL
    if master is None:
        master = os.environ.get("TM_SPARK_MASTER", "local[2]")
    
    # Get JAR configuration
    jars_config = get_spark_jars_config()

    # IMPORTANT: Java's AWS SDK reads region from either:
    # - Java system property: aws.region
    # - process environment variables (AWS_REGION / AWS_DEFAULT_REGION)
    #
    # In PySpark, the JVM may be started before Python code runs, so mutating
    # os.environ here is NOT reliably visible to the JVM (System.getenv).
    #
    # We still set env vars for consistency, but we *also* set Java system props
    # via Spark driver/executor extraJavaOptions to make region resolution robust.
    os.environ.setdefault("AWS_REGION", config.s3_region)
    os.environ.setdefault("AWS_DEFAULT_REGION", config.s3_region)
    if config.s3_access_key:
        os.environ.setdefault("AWS_ACCESS_KEY_ID", config.s3_access_key)
    if config.s3_secret_key:
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", config.s3_secret_key)
    
    # Build Spark session
    spark_builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.jars", jars_config)
        # Ensure AWS region is visible to AWS SDK inside the JVM
        .config("spark.driver.extraJavaOptions", f"-Daws.region={config.s3_region}")
        .config("spark.executor.extraJavaOptions", f"-Daws.region={config.s3_region}")
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        # Iceberg catalog configuration
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", config.iceberg_catalog_type)
        .config("spark.sql.catalog.iceberg.warehouse", config.iceberg_warehouse_silver)
        .config(
            "spark.sql.catalog.iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.iceberg.s3.region", config.s3_region)
        .config(
            "spark.sql.catalog.iceberg.s3.path-style-access",
            str(config.s3_force_path_style).lower()
        )
    )
    
    # Add REST URI for REST catalog (local development)
    if config.iceberg_rest_uri:
        spark_builder = spark_builder.config(
            "spark.sql.catalog.iceberg.uri",
            config.iceberg_rest_uri
        )
    
    # Add S3 endpoint and credentials for MinIO (local development)
    if config.s3_endpoint:
        spark_builder = (
            spark_builder
            .config("spark.sql.catalog.iceberg.s3.endpoint", config.s3_endpoint)
            # Hadoop S3A configuration for reading files
            .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                str(config.s3_force_path_style).lower()
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.region", config.s3_region)
        )
    
    # Add credentials if provided
    if config.s3_access_key:
        spark_builder = (
            spark_builder
            .config("spark.sql.catalog.iceberg.s3.access-key-id", config.s3_access_key)
            .config("spark.hadoop.fs.s3a.access.key", config.s3_access_key)
        )
    if config.s3_secret_key:
        spark_builder = (
            spark_builder
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", config.s3_secret_key)
            .config("spark.hadoop.fs.s3a.secret.key", config.s3_secret_key)
        )
    
    # Create the session
    spark = spark_builder.getOrCreate()
    
    # Configure session
    spark.sparkContext.setLogLevel(log_level)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    
    return spark

