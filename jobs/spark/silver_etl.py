"""Silver ETL job - transforms Bronze CSV into Silver Iceberg table (long format).

This script is run via spark-submit inside the Spark container.
It reads Bronze CSV files from the Landing Zone, transforms them into a long-format table
(one row per GPS fix), and writes to an Iceberg table in the Source Store.

Data Flow: Landing Zone CSV → Spark DataFrame → Iceberg Table (silver.fact_telemetry_event)
"""

from __future__ import annotations

import argparse
import re
import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, Window


def _col(name: str):
    """Escape column name containing brackets/dots for Spark SQL."""
    return F.col(f"`{name}`")


def discover_location_indices(columns: list[str]) -> list[int]:
    """Find all location array indices present in CSV columns (locations[N].startTime)."""
    idxs: set[int] = set()
    pat = re.compile(r"^locations\[(\d+)\]\.startTime$")
    for c in columns:
        m = pat.match(c)
        if m:
            idxs.add(int(m.group(1)))
    return sorted(idxs)


def main():
    """Main entry point for Silver ETL Spark job."""
    parser = argparse.ArgumentParser(
        description="Silver ETL: Landing Zone CSV → Iceberg table"
    )
    parser.add_argument("--course-id", required=True, help="Course identifier")
    parser.add_argument("--ingest-date", required=True, help="Ingest date (YYYY-MM-DD)")
    parser.add_argument(
        "--bronze-prefix", required=True, help="Landing zone key prefix"
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SILVER ETL: {args.course_id} / {args.ingest_date}")
    print(f"{'='*60}")

    # Configuration
    s3_endpoint = os.environ.get("TM_S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.environ.get("TM_S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.environ.get("TM_S3_SECRET_KEY", "minioadmin")
    bucket_landing = os.environ.get("TM_BUCKET_LANDING", "tm-lakehouse-landing-zone")
    bucket_source = os.environ.get("TM_BUCKET_SOURCE", "tm-lakehouse-source-store")
    bucket_quarantine = os.environ.get(
        "TM_BUCKET_QUARANTINE", "tm-lakehouse-quarantine"
    )
    bucket_observability = os.environ.get(
        "TM_BUCKET_OBSERVABILITY", "tm-lakehouse-observability"
    )
    iceberg_warehouse = os.environ.get(
        "TM_ICEBERG_WAREHOUSE_SILVER", f"s3a://{bucket_source}/warehouse"
    )
    db_silver = os.environ.get("TM_DB_SILVER", "silver")
    s3_region = os.environ.get("TM_S3_REGION", "us-east-1")

    # Create Spark session with Iceberg configured
    spark = (
        SparkSession.builder.appName(f"silver_etl_{args.course_id}_{args.ingest_date}")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse)
        .config(
            "spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.iceberg.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.iceberg.s3.access-key-id", s3_access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", s3_secret_key)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region", s3_region)
        # Hadoop S3A configuration for reading Landing Zone CSVs
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.region", s3_region)
        .getOrCreate()
    )

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    landing_uri = f"s3a://{bucket_landing}/{args.bronze_prefix}"
    print(f"  Reading: {landing_uri}")

    # Read Landing Zone CSV
    try:
        df = (
            spark.read.option("header", True)
            .option("escape", '"')
            .option("multiLine", False)
            .csv(landing_uri)
        )
    except Exception as e:
        print(f"  ❌ Failed to read Landing Zone data: {e}")
        raise

    row_count = df.count()
    print(f"  Found {row_count:,} rows in Landing Zone")

    location_idxs = discover_location_indices(df.columns)
    if not location_idxs:
        raise ValueError(
            "No locations[i].startTime columns found; cannot build Silver long table"
        )
    print(f"  Found {len(location_idxs)} location indices")

    # Add round-level fields
    base = (
        df.withColumnRenamed("_id", "round_id")
        .withColumn("course_id", F.lit(args.course_id))
        .withColumn("ingest_date", F.lit(args.ingest_date))
        .withColumn("round_start_time", F.to_timestamp(_col("startTime")))
    )

    # Build array<struct> for all location indices and explode to long format
    loc_structs = []
    for i in location_idxs:

        def c(suffix: str):
            name = f"locations[{i}].{suffix}"
            return _col(name) if name in df.columns else F.lit(None)

        loc_structs.append(
            F.struct(
                F.lit(i).alias("location_index"),
                c("hole").cast("int").alias("hole_number"),
                c("sectionNumber").cast("int").alias("section_number"),
                c("holeSection").cast("int").alias("hole_section"),
                c("startTime").cast("double").alias("start_offset_seconds"),
                c("date").alias("fix_time_iso"),
                c("fixCoordinates[0]").cast("double").alias("longitude"),
                c("fixCoordinates[1]").cast("double").alias("latitude"),
                c("isProjected").cast("boolean").alias("is_projected"),
                c("isProblem").cast("boolean").alias("is_problem"),
                c("isCache").cast("boolean").alias("is_cache"),
                c("paceGap").cast("double").alias("pace_gap"),
                c("positionalGap").cast("double").alias("positional_gap"),
                c("pace").cast("double").alias("pace"),
                c("batteryPercentage").cast("double").alias("battery_percentage"),
            )
        )

    long_df = base.withColumn("location", F.explode(F.array(*loc_structs)))

    # Derive fix_timestamp from ISO column or round_start + offset
    fix_ts = F.coalesce(
        F.to_timestamp(F.col("location.fix_time_iso")),
        F.from_unixtime(
            F.col("round_start_time").cast("double")
            + F.col("location.start_offset_seconds")
        ).cast("timestamp"),
    )

    out = (
        long_df.select(
            "round_id",
            "course_id",
            "ingest_date",
            fix_ts.alias("fix_timestamp"),
            F.col("location.location_index"),
            F.col("location.hole_number"),
            F.col("location.section_number"),
            F.col("location.hole_section"),
            F.col("location.longitude"),
            F.col("location.latitude"),
            F.col("location.is_cache"),
            F.col("location.is_projected"),
            F.col("location.is_problem"),
            F.col("location.pace_gap"),
            F.col("location.positional_gap"),
            F.col("location.pace"),
            F.col("location.battery_percentage"),
        )
        .withColumn("event_date", F.to_date("fix_timestamp"))
        .withColumn(
            "geometry_wkt",
            F.when(
                F.col("longitude").isNotNull() & F.col("latitude").isNotNull(),
                F.concat(
                    F.lit("POINT("),
                    F.col("longitude").cast("string"),
                    F.lit(" "),
                    F.col("latitude").cast("string"),
                    F.lit(")"),
                ),
            ),
        )
    )

    # Drop rows with no timestamp
    out = out.filter(F.col("fix_timestamp").isNotNull())

    # Dedup: prefer is_cache=true for same (round_id, fix_timestamp)
    w = Window.partitionBy("round_id", "fix_timestamp").orderBy(
        F.col("is_cache").desc_nulls_last()
    )
    out = (
        out.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Split valid vs invalid coordinates (quarantine bad rows)
    invalid = out.filter(
        (
            F.col("longitude").isNotNull()
            & ((F.col("longitude") < -180) | (F.col("longitude") > 180))
        )
        | (
            F.col("latitude").isNotNull()
            & ((F.col("latitude") < -90) | (F.col("latitude") > 90))
        )
    )
    valid = out.subtract(invalid)

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # Write quarantined rows
    invalid_count = invalid.count()
    if invalid_count > 0:
        quarantine_path = f"s3a://{bucket_quarantine}/silver/course_id={args.course_id}/ingest_date={args.ingest_date}/run_id={run_id}"
        invalid.write.mode("overwrite").json(quarantine_path)
        print(f"  ⚠  Quarantined: {invalid_count:,} invalid rows → {quarantine_path}")

    # Create namespace if needed
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{db_silver}")

    table = f"iceberg.{db_silver}.fact_telemetry_event"
    valid_count = valid.count()
    print(f"  Writing {valid_count:,} valid rows...")

    # Check if table exists
    try:
        spark.sql(f"DESCRIBE TABLE {table}")
        table_exists = True
    except Exception:
        table_exists = False

    if table_exists:
        # APPEND mode for incremental ingestion (required for 650 courses x 7 years!)
        # First delete existing data for this course/ingest_date to ensure idempotency
        spark.sql(
            f"""
            DELETE FROM {table} 
            WHERE course_id = '{args.course_id}' 
            AND ingest_date = '{args.ingest_date}'
        """
        )
        print(
            f"  → Deleted existing data for course={args.course_id}, ingest_date={args.ingest_date}"
        )

        # Then append new data
        valid.writeTo(table).append()
        print(f"  → Appended {valid_count:,} rows")
    else:
        # Create table with partitioning on first run
        valid.writeTo(table).using("iceberg").partitionedBy(
            "course_id", "event_date"
        ).create()
        print(f"  → Created table with {valid_count:,} rows")

    print(f"  ✅ DONE: {valid_count:,} rows → {table}")

    # Write run summary
    summary_path = f"s3a://{bucket_observability}/silver/course_id={args.course_id}/ingest_date={args.ingest_date}/run_id={run_id}"
    summary = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "course_id": args.course_id,
                "ingest_date": args.ingest_date,
                "landing_uri": landing_uri,
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "table": table,
            }
        ]
    )
    summary.coalesce(1).write.mode("overwrite").json(summary_path)
    print(f"  → Summary: {summary_path}")
    print(f"{'='*60}\n")

    spark.stop()


if __name__ == "__main__":
    main()
