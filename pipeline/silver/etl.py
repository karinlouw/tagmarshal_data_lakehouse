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
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StructType

# Ensure shared lib is importable when run via spark-submit
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from tm_lakehouse.config import TMConfig
from tm_lakehouse.spark_utils import create_iceberg_spark_session
from tm_lakehouse.constants import COORD_BOUNDS


def _col(name: str):
    """Escape column name containing brackets/dots for Spark SQL."""
    return F.col(f"`{name}`")


def detect_file_format(spark: SparkSession, uri: str) -> str:
    """Detect if input is CSV or JSON based on file extension or content."""
    # Check if any JSON files exist
    try:
        json_uri = uri if uri.endswith(".json") else f"{uri}/*.json"
        json_count = spark.read.format("binaryFile").load(json_uri).limit(1).count()
        if json_count > 0:
            return "json"
    except Exception:
        pass

    # Check for CSV files
    try:
        csv_uri = uri if uri.endswith(".csv") else f"{uri}/*.csv"
        csv_count = spark.read.format("binaryFile").load(csv_uri).limit(1).count()
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

    # Configuration (centralized via TMConfig to avoid scattered defaults)
    cfg = TMConfig.from_env()
    bucket_landing = cfg.bucket_landing
    bucket_quarantine = cfg.bucket_quarantine
    bucket_observability = cfg.bucket_observability
    db_silver = cfg.db_silver

    # Coordinate validation bounds (from constants or config)
    lon_min = (
        cfg.coord_min_lon
        if cfg.coord_min_lon != COORD_BOUNDS["lon_min"]
        else COORD_BOUNDS["lon_min"]
    )
    lon_max = (
        cfg.coord_max_lon
        if cfg.coord_max_lon != COORD_BOUNDS["lon_max"]
        else COORD_BOUNDS["lon_max"]
    )
    lat_min = (
        cfg.coord_min_lat
        if cfg.coord_min_lat != COORD_BOUNDS["lat_min"]
        else COORD_BOUNDS["lat_min"]
    )
    lat_max = (
        cfg.coord_max_lat
        if cfg.coord_max_lat != COORD_BOUNDS["lat_max"]
        else COORD_BOUNDS["lat_max"]
    )

    # Create Spark session using centralized utility
    spark = create_iceberg_spark_session(
        app_name=f"silver_etl_{args.course_id}_{args.ingest_date}",
        config=cfg,
        log_level="WARN",
    )

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
            # IMPORTANT:
            # Some Tagmarshal CSV exports can have different column sets and/or ordering
            # across files (e.g. locations[0..N] varies). Reading a glob in one shot can
            # misalign columns across files and silently produce NULLs for critical fields.
            #
            # We therefore read each CSV file independently (using its header), then
            # union by column name (allowing missing columns).
            csv_files = (
                spark.read.format("binaryFile")
                .load(csv_path)
                .select("path")
                .distinct()
                .collect()
            )
            csv_file_paths = sorted([r["path"] for r in csv_files])
            if not csv_file_paths:
                raise ValueError(f"No CSV files found at: {csv_path}")

            dfs = []
            for p in csv_file_paths:
                dfs.append(
                    spark.read.option("header", True)
                    .option("escape", '"')
                    .option("multiLine", False)
                    .csv(p)
                )

            landing_df = dfs[0]
            for df in dfs[1:]:
                landing_df = landing_df.unionByName(df, allowMissingColumns=True)
    except Exception as e:
        print(f"  ❌ Failed to read Landing Zone data: {e}")
        raise

    row_count = landing_df.count()
    print(f"  Found {row_count:,} rows in Landing Zone")

    # Sanity check: ensure source course matches requested course_id
    if "course" in landing_df.columns:
        course_mismatch = (
            landing_df.filter(_col("course") != F.lit(args.course_id)).limit(1).count()
        )
        if course_mismatch:
            print(
                f"  ⚠ Course mismatch detected in source data (column 'course' != {args.course_id}). Data will be tagged with requested course_id."
            )

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
        """
        Safely reference a possibly Mongo-style field.

        Some JSON exports encode values as structs like {"$oid": "..."} or {"$date": "..."}.
        Critically, not all structs have both keys; referencing a missing struct field causes a
        Spark AnalysisException at planning time. We therefore only reference subfields that
        actually exist in the schema.
        """
        if name not in landing_df.columns:
            return F.lit(None)

        dt = landing_df.schema[name].dataType
        if isinstance(dt, StructType):
            subfields = {f.name for f in dt.fields}
            candidates = []
            if "$oid" in subfields:
                candidates.append(F.col(f"{name}.$oid"))
            if "$date" in subfields:
                candidates.append(F.col(f"{name}.$date"))
            if not candidates:
                return F.lit(None)
            if len(candidates) == 1:
                return candidates[0]
            return F.coalesce(*candidates)

        return F.col(name)

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

    # Handle endTime similar to startTime
    if "endTime" in landing_df.columns:
        end_time_type = str(landing_df.schema["endTime"].dataType)
        if "StructType" in end_time_type:
            round_end_col = F.to_timestamp(F.col("endTime.$date"))
        else:
            round_end_col = F.to_timestamp(_col("endTime"))
    else:
        round_end_col = F.lit(None)

    base_df = (
        base_df.withColumn("course_id", F.lit(args.course_id))
        .withColumn("ingest_date", F.lit(args.ingest_date))
        .withColumn("round_start_time", round_start_col)
        .withColumn("round_end_time", round_end_col)
        # Round configuration fields (critical for multi-nine and shotgun starts)
        .withColumn("start_hole", safe_col("startHole").cast("int"))
        .withColumn("start_section", safe_col("startSection").cast("int"))
        .withColumn("end_section", safe_col("endSection").cast("int"))
        .withColumn("is_nine_hole", safe_col("isNineHole").cast("boolean"))
        .withColumn("current_nine", safe_col("currentNine").cast("int"))
        .withColumn("goal_time", safe_col("goalTime").cast("int"))
        .withColumn("is_complete", safe_col("complete").cast("boolean"))
        # Additional round-level fields from original data
        .withColumn("device", safe_col("device"))
        .withColumn("first_fix", safe_col("firstFix"))
        .withColumn("last_fix", safe_col("lastFix"))
        .withColumn("goal_name", safe_col("goalName"))
        .withColumn("goal_time_fraction", safe_col("goalTimeFraction").cast("double"))
        .withColumn("is_incomplete", safe_col("isIncomplete").cast("boolean"))
        .withColumn("is_secondary", safe_col("isSecondary").cast("boolean"))
        .withColumn("is_auto_assigned", safe_col("isAutoAssigned").cast("boolean"))
        .withColumn("last_section_start", safe_col("lastSectionStart").cast("double"))
        .withColumn("current_section", safe_col("currentSection").cast("int"))
        .withColumn("current_hole", safe_col("currentHole").cast("int"))
        .withColumn("current_hole_section", safe_col("currentHoleSection").cast("int"))
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

    # NOTE: We do NOT filter out NULL values - all data is preserved, including rows where
    # hole_number and section_number are both NULL. This ensures no data loss.

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
        # Track CSV padding rows explicitly (we preserve them, but downstream models can ignore them)
        (
            F.col("location.hole_number").isNull()
            & F.col("location.section_number").isNull()
        ).alias("is_location_padding"),
        # Round-level timestamps
        "round_start_time",
        "round_end_time",
        # Round configuration (for proper multi-nine and shotgun start handling)
        "start_hole",
        "start_section",
        "end_section",
        "is_nine_hole",
        "current_nine",
        "goal_time",
        "is_complete",
        # Additional round-level fields
        "device",
        "first_fix",
        "last_fix",
        "goal_name",
        "goal_time_fraction",
        "is_incomplete",
        "is_secondary",
        "is_auto_assigned",
        "last_section_start",
        "current_section",
        "current_hole",
        "current_hole_section",
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

    # Add enrichment columns for round duration and date parts
    # Round duration in minutes (from round_start_time to round_end_time)
    telemetry_df = telemetry_df.withColumn(
        "round_duration_minutes",
        F.when(
            F.col("round_start_time").isNotNull() & F.col("round_end_time").isNotNull(),
            F.round(
                (
                    F.unix_timestamp("round_end_time")
                    - F.unix_timestamp("round_start_time")
                )
                / 60.0,
                2,
            ),
        ),
    )

    # Date parts from fix_timestamp for easier analysis
    telemetry_df = (
        telemetry_df.withColumn("event_year", F.year("fix_timestamp"))
        .withColumn("event_month", F.month("fix_timestamp"))
        .withColumn("event_day", F.dayofmonth("fix_timestamp"))
        .withColumn(
            "event_weekday", F.dayofweek("fix_timestamp")
        )  # 1=Sun, 7=Sat in Spark
    )

    # Derive which nine the location is in based on topology mapping
    # This replaces the hardcoded logic with a data-driven approach using dim_facility_topology
    #
    # Nine number derivation priority:
    # 1. current_nine from source data (best for 9-hole loop courses)
    # 2. nine_number_topo from topology table join (based on section ranges)
    # 3. Fallback: derive from hole_number for 18-hole courses (holes 1-9 = nine 1, 10-18 = nine 2)
    # 4. Fallback: derive from section_number ranges for 27-hole courses
    seed_path = os.path.join(
        os.path.dirname(__file__), "seeds", "dim_facility_topology.csv"
    )

    # Fallback logic for when topology is missing:
    # - For 18-hole courses (hole_number >= 10): use hole_number to determine nine
    # - For 27-hole courses (hole_number <= 9 but many sections): use section_number bands
    nine_from_hole_number = F.when(
        F.col("hole_number") >= 10, F.lit(2)  # Holes 10-18 are nine 2
    ).when(
        F.col("hole_number").isNotNull(),
        F.lit(1),  # Holes 1-9 are nine 1 (for 18-hole courses)
    )

    nine_from_section_number = (
        F.when(F.col("section_number") <= 27, F.lit(1))
        .when(F.col("section_number") <= 54, F.lit(2))
        .when(F.col("section_number") <= 81, F.lit(3))
        .otherwise(F.lit(1))
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

        # Derive nine_number with priority:
        # 1. current_nine from source (for 9-hole loop courses tracking loop 1 vs 2)
        # 2. nine_number_topo from topology join
        # 3. Fallback: derive from hole_number (for 18-hole courses)
        # 4. Fallback: derive from section_number (for 27-hole courses)
        telemetry_df = telemetry_df.withColumn(
            "nine_number",
            F.coalesce(
                F.col("current_nine"),
                F.col("nine_number_topo"),
                nine_from_hole_number,
                nine_from_section_number,
            ),
        ).drop("nine_number_topo")
    else:
        print(f"  ⚠ Topology seed not found at {seed_path}, using fallback logic")
        telemetry_df = telemetry_df.withColumn(
            "nine_number",
            F.coalesce(
                F.col("current_nine"), nine_from_hole_number, nine_from_section_number
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

    # Dedup: prefer is_cache=true for same (round_id, fix_timestamp, location_index)
    # IMPORTANT: fix_timestamp can be NULL (we preserve NULLs), so we must include
    # location_index to avoid collapsing all NULL-timestamp rows into a single record.
    window_spec = Window.partitionBy(
        "round_id", "fix_timestamp", "location_index"
    ).orderBy(
        F.col("is_cache").desc_nulls_last(),
        F.col("is_projected").asc_nulls_last(),
        F.col("battery_percentage").desc_nulls_last(),
    )
    telemetry_df = (
        telemetry_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Split valid vs invalid coordinates (quarantine bad rows)
    # Todo: What is defined of invalid coordinates?
    telemetry_df = telemetry_df.withColumn(
        "is_invalid_coord",
        (
            F.col("longitude").isNotNull()
            & (
                (F.col("longitude") < F.lit(lon_min))
                | (F.col("longitude") > F.lit(lon_max))
            )
        )
        | (
            F.col("latitude").isNotNull()
            & (
                (F.col("latitude") < F.lit(lat_min))
                | (F.col("latitude") > F.lit(lat_max))
            )
        ),
    )
    invalid = telemetry_df.filter("is_invalid_coord")
    valid = telemetry_df.filter(~F.col("is_invalid_coord"))

    # Todo: Updage deprecated datetime
    from datetime import timezone

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Write quarantined rows
    invalid = invalid.drop("is_invalid_coord")
    valid = valid.drop("is_invalid_coord")

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

        # Schema evolution: if the incoming dataframe has new columns (e.g. local dev adds flags),
        # evolve the Iceberg table schema before append to avoid arity mismatch errors.
        try:
            existing_schema = spark.table(table).schema
            existing_cols = {f.name for f in existing_schema.fields}
            if (
                "is_location_padding" in valid.columns
                and "is_location_padding" not in existing_cols
            ):
                spark.sql(f"ALTER TABLE {table} ADD COLUMN is_location_padding boolean")
                # Refresh schema after evolution
                existing_schema = spark.table(table).schema
        except Exception as e:
            print(f"  ⚠  Warning: could not check/evolve table schema: {e}")

        # Align dataframe columns to the table schema (order + drop unknown extras)
        try:
            table_cols = [f.name for f in spark.table(table).schema.fields]
            valid = valid.select(*[c for c in table_cols if c in valid.columns])
        except Exception as e:
            print(f"  ⚠  Warning: could not align columns to table schema: {e}")

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
