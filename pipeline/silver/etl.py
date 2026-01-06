"""Silver ETL job - transforms Bronze CSV/JSON into Silver Iceberg table (long format).

This script is run via spark-submit inside the Spark container.
It reads Bronze CSV or JSON files from the Landing Zone, transforms them into a long-format
table (one row per GPS fix), and writes to an Iceberg table in the Source Store.

Data Flow: Landing Zone (CSV or JSON) → Spark DataFrame → Iceberg Table (silver.fact_telemetry_event)

Supports two input formats:
- CSV: Flattened columns like locations[0].hole, locations[0].startTime
- JSON: Nested MongoDB format with locations array
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


def detect_file_format(spark: SparkSession, uri: str) -> str:
    """Detect if input is CSV or JSON based on file extension or content."""
    # Check if any JSON files exist
    try:
        json_uri = uri if uri.endswith(".json") else f"{uri}/*.json"
        json_count = spark.read.format("binaryFile").load(json_uri).count()
        if json_count > 0:
            return "json"
    except Exception:
        pass

    # Check for CSV files
    try:
        csv_uri = uri if uri.endswith(".csv") else f"{uri}/*.csv"
        csv_count = spark.read.format("binaryFile").load(csv_uri).count()
        if csv_count > 0:
            return "csv"
    except Exception:
        pass

    # Default to CSV (let it fail naturally if neither exists)
    return "csv"


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

    # Detect file format (CSV or JSON)
    file_format = detect_file_format(spark, landing_uri)
    print(f"  Format detected: {file_format.upper()}")

    # Read Landing Zone data based on format
    # IMPORTANT: When both JSON and CSV exist, read ONLY the correct file type
    try:
        if file_format == "json":
            # Read only JSON files (not CSV even if present in same directory)
            # Todo: check what this means
            json_path = (
                f"{landing_uri}/*.json"
                if not landing_uri.endswith(".json")
                else landing_uri
            )
            print(f"  Reading JSON: {json_path}")
            landing_df = spark.read.option("multiLine", True).json(json_path)
        else:
            # Read only CSV files (not JSON even if present in same directory)
            csv_path = (
                f"{landing_uri}/*.csv"
                if not landing_uri.endswith(".csv")
                else landing_uri
            )
            print(f"  Reading CSV: {csv_path}")
            landing_df = (
                spark.read.option("header", True)
                .option("escape", '"')
                .option("multiLine", False)
                .csv(csv_path)
            )
    except Exception as e:
        print(f"  ❌ Failed to read Landing Zone data: {e}")
        raise

    row_count = landing_df.count()
    print(f"  Found {row_count:,} rows in Landing Zone")

    # For JSON with nested locations, we process differently
    if file_format == "json" and "locations" in landing_df.columns:
        location_idxs = None  # Signal to use JSON path
        print(f"  Using nested JSON locations array")
    else:
        location_idxs = discover_location_indices(landing_df.columns)
        if not location_idxs:
            raise ValueError(
                "No locations[i].startTime columns found; cannot build Silver long table"
            )
        print(f"  Found {len(location_idxs)} location indices")

    # Add round-level fields
    # Helper to safely get column (handles both CSV and JSON/MongoDB format)
    # Todo: check what this means
    def safe_col(name: str):
        if name in landing_df.columns:
            col_type = str(landing_df.schema[name].dataType)
            if "StructType" in col_type:
                # MongoDB format: {"$oid": "..."} or {"$date": "..."}
                return F.coalesce(F.col(f"{name}.$oid"), F.col(f"{name}.$date"))
            return F.col(name)
        return F.lit(None)

    # Handle MongoDB date format for startTime
    # Todo: Check that we normalise time and handle timezone correctly
    if "startTime" in landing_df.columns:
        start_time_type = str(landing_df.schema["startTime"].dataType)
        if "StructType" in start_time_type:
            # MongoDB format: {"$date": "..."}
            round_start_col = F.to_timestamp(F.col("startTime.$date"))
        else:
            round_start_col = F.to_timestamp(_col("startTime"))
    else:
        round_start_col = F.lit(None)

    # Apply round_id logic if we haven't already (handles _id)
    if "_id" in landing_df.columns and "round_id" not in landing_df.columns:
        # Check if _id is a struct (MongoDB format) or string
        id_type = str(landing_df.schema["_id"].dataType)
        if "StructType" in id_type:
            base_df = landing_df.withColumn("round_id", F.col("_id.$oid"))
        else:
            base_df = landing_df.withColumnRenamed("_id", "round_id")
    else:
        base_df = landing_df

    # Ensure round_id exists (if it wasn't in source and we didn't map _id)
    if "round_id" not in base_df.columns:
        base_df = base_df.withColumn("round_id", F.lit(None))

    base_df = (
        base_df.withColumn("course_id", F.lit(args.course_id))
        .withColumn("ingest_date", F.lit(args.ingest_date))
        .withColumn("round_start_time", round_start_col)
        # Round configuration fields (critical for multi-nine and shotgun starts)
        .withColumn("start_hole", safe_col("startHole").cast("int"))
        .withColumn("start_section", safe_col("startSection").cast("int"))
        .withColumn("end_section", safe_col("endSection").cast("int"))
        .withColumn("is_nine_hole", safe_col("isNineHole").cast("boolean"))
        .withColumn("current_nine", safe_col("currentNine").cast("int"))
        .withColumn("goal_time", safe_col("goalTime").cast("int"))
        .withColumn("is_complete", safe_col("complete").cast("boolean"))
    )

    # Handle JSON with nested locations array vs CSV with flattened columns
    if location_idxs is None:
        # JSON format: explode nested locations array directly
        # posexplode returns (position, value) tuple - must select both separately
        long_df = (
            base_df.select(
                "*", F.posexplode("locations").alias("location_index", "loc")
            )
            .drop("locations")
            .withColumn(
                "location",
                F.struct(
                    F.col("location_index"),
                    F.col("loc.hole").cast("int").alias("hole_number"),
                    F.col("loc.sectionNumber").cast("int").alias("section_number"),
                    F.col("loc.holeSection").cast("int").alias("hole_section"),
                    F.col("loc.startTime").cast("double").alias("start_offset_seconds"),
                    F.lit(None).alias("fix_time_iso"),  # JSON doesn't have this field
                    F.col("loc.fixCoordinates")
                    .getItem(0)
                    .cast("double")
                    .alias("longitude"),
                    F.col("loc.fixCoordinates")
                    .getItem(1)
                    .cast("double")
                    .alias("latitude"),
                    F.col("loc.isProjected").cast("boolean").alias("is_projected"),
                    F.col("loc.isProblem").cast("boolean").alias("is_problem"),
                    F.col("loc.isCache").cast("boolean").alias("is_cache"),
                    F.round(F.col("loc.paceGap").cast("double"), 3).alias("pace_gap"),
                    F.round(F.col("loc.positionalGap").cast("double"), 3).alias(
                        "positional_gap"
                    ),
                    F.round(F.col("loc.pace").cast("double"), 3).alias("pace"),
                    F.col("loc.batteryPercentage")
                    .cast("double")
                    .alias("battery_percentage"),
                ),
            )
            .drop("loc", "location_index")
        )
    else:
        # CSV format: build array<struct> for all location indices
        loc_structs = []
        for i in location_idxs:

            def get_col(suffix: str):
                name = f"locations[{i}].{suffix}"
                return _col(name) if name in landing_df.columns else F.lit(None)

            loc_structs.append(
                F.struct(
                    F.lit(i).alias("location_index"),
                    get_col("hole").cast("int").alias("hole_number"),
                    get_col("sectionNumber").cast("int").alias("section_number"),
                    get_col("holeSection").cast("int").alias("hole_section"),
                    get_col("startTime").cast("double").alias("start_offset_seconds"),
                    get_col("date").alias("fix_time_iso"),
                    get_col("fixCoordinates[0]").cast("double").alias("longitude"),
                    get_col("fixCoordinates[1]").cast("double").alias("latitude"),
                    get_col("isProjected").cast("boolean").alias("is_projected"),
                    get_col("isProblem").cast("boolean").alias("is_problem"),
                    get_col("isCache").cast("boolean").alias("is_cache"),
                    F.round(get_col("paceGap").cast("double"), 3).alias("pace_gap"),
                    F.round(get_col("positionalGap").cast("double"), 3).alias(
                        "positional_gap"
                    ),
                    F.round(get_col("pace").cast("double"), 3).alias("pace"),
                    get_col("batteryPercentage")
                    .cast("double")
                    .alias("battery_percentage"),
                )
            )

        long_df = base_df.withColumn("location", F.explode(F.array(*loc_structs)))

    # Todo: we need to investigate this more, it seems like we are losing data here.

    # Filter out empty location slots (rows where key fields are NULL)
    # This removes "padding" rows from CSVs with more slots than actual data
    long_df = long_df.filter(
        F.col("location.hole_number").isNotNull()
        | F.col("location.section_number").isNotNull()
    )

    # Derive fix_timestamp from ISO column or round_start + offset
    fix_ts = F.coalesce(
        F.to_timestamp(F.col("location.fix_time_iso")),
        F.from_unixtime(
            F.col("round_start_time").cast("double")
            + F.col("location.start_offset_seconds")
        ).cast("timestamp"),
    )

    # Initial selection and transformation
    telemetry_df = long_df.select(
        "round_id",
        "course_id",
        "ingest_date",
        fix_ts.alias("fix_timestamp"),
        # Round configuration (for proper multi-nine and shotgun start handling)
        "start_hole",
        "start_section",
        "end_section",
        "is_nine_hole",
        "current_nine",
        "goal_time",
        "is_complete",
        # Location fields
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
    ).withColumn("event_date", F.to_date("fix_timestamp"))

    # Derive which nine the location is in based on topology mapping
    # This replaces the hardcoded logic with a data-driven approach using dim_facility_topology
    seed_path = os.path.join(
        os.path.dirname(__file__), "seeds", "dim_facility_topology.csv"
    )

    if os.path.exists(seed_path):
        print(f"  Reading Topology: {seed_path}")
        # Read CSV and cast columns
        topology_df = (
            spark.read.option("header", True)
            .csv(f"file://{seed_path}")
            .withColumn("section_start", F.col("section_start").cast("int"))
            .withColumn("section_end", F.col("section_end").cast("int"))
            .withColumn("nine_number_topo", F.col("nine_number").cast("int"))
            .select("facility_id", "section_start", "section_end", "nine_number_topo")
        )

        # Broadcast the topology table as it's small
        topology_df = F.broadcast(topology_df)

        # Perform join
        # We use a left join to preserve telemetry rows even if topology is missing
        telemetry_df = telemetry_df.join(
            topology_df,
            (F.col("course_id") == F.col("facility_id"))
            & (F.col("section_number") >= F.col("section_start"))
            & (F.col("section_number") <= F.col("section_end")),
            "left",
        ).drop("facility_id", "section_start", "section_end")

        # Handle cases where no topology match was found (missing from CSV) or where
        # the source data explicitly provides 'current_nine' (overriding inference).
        # Priority:
        # 1. 'current_nine' from source (if available and trusted)
        # 2. 'nine_number_topo' from topology join
        # 3. Fallback heuristic
        telemetry_df = telemetry_df.withColumn(
            "nine_number",
            F.coalesce(
                F.col("current_nine"),
                F.col("nine_number_topo"),
                F.when(F.col("section_number") <= 27, 1)
                .when(F.col("section_number") <= 54, 2)
                .otherwise(3),
            ),
        ).drop("nine_number_topo")
    else:
        print(f"  ⚠ Topology seed not found at {seed_path}, using fallback logic")
        telemetry_df = telemetry_df.withColumn(
            "nine_number",
            F.coalesce(
                F.col("current_nine"),
                F.when(F.col("section_number") <= 27, 1)
                .when(F.col("section_number") <= 54, 2)
                .otherwise(3),
            ),
        )

    # Continue with remaining transformations
    telemetry_df = (
        telemetry_df.withColumn(
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
        # Add flag to track missing timestamps (for data quality analysis)
        .withColumn("is_timestamp_missing", F.col("fix_timestamp").isNull())
    )

    # Dedup: prefer is_cache=true for same (round_id, fix_timestamp)
    # This partitions data by round and time, then orders by is_cache desc (True first).
    # We take the first row, so we prefer the cached version if duplicates exist.
    window_spec = Window.partitionBy("round_id", "fix_timestamp").orderBy(
        F.col("is_cache").desc_nulls_last()
    )
    telemetry_df = (
        telemetry_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Split valid vs invalid coordinates (quarantine bad rows)
    # Todo: What is defined of invalid coordinates?
    invalid = telemetry_df.filter(
        (
            F.col("longitude").isNotNull()
            & ((F.col("longitude") < -180) | (F.col("longitude") > 180))
        )
        | (
            F.col("latitude").isNotNull()
            & ((F.col("latitude") < -90) | (F.col("latitude") > 90))
        )
    )
    valid = telemetry_df.subtract(invalid)

    # Todo: Updage deprecated datetime
    from datetime import timezone

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

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
