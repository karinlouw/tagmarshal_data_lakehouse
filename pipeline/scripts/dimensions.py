"""Consolidated "dimension utilities" script.

This replaces:
- generate_topology.py
- seed_topology.py
- seed_course_profile.py

Design goals:
- Single place for Spark/Iceberg session config (reuse tm_lakehouse.spark_utils)
- Support both local workflows (generate CSV → edit → seed) and automation (refresh table)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

# Ensure shared lib is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from tm_lakehouse.config import TMConfig
from tm_lakehouse.spark_utils import create_iceberg_spark_session


@dataclass(frozen=True)
class CourseTopology:
    """Inferred topology for a single course."""

    course_id: str
    course_type: str  # "9-hole", "18-hole", "27-hole", "18-hole-loop"
    nines: List[
        Tuple[int, int, int]
    ]  # List of (nine_number, section_start, section_end)


def infer_course_type(max_hole: int, max_section: int) -> str:
    """Determine course type from observed data ranges."""
    if max_hole >= 10:
        return "18-hole"
    if max_section > 54:
        return "27-hole"
    if max_section > 27:
        # Could be 9-hole playing 18, or 18-hole with odd encoding
        return "18-hole-loop"
    return "9-hole"


def infer_nine_boundaries_from_holes(
    course_df: DataFrame, max_hole: int
) -> List[Tuple[int, int, int]]:
    """Infer nine boundaries for courses where hole_number is continuous (1-18+)."""
    nines: List[Tuple[int, int, int]] = []

    # Front nine: holes 1-9
    front = (
        course_df.filter((F.col("hole_number") >= 1) & (F.col("hole_number") <= 9))
        .agg(
            F.min("section_number").alias("section_start"),
            F.max("section_number").alias("section_end"),
        )
        .collect()
    )
    if front and front[0]["section_start"] is not None:
        nines.append((1, int(front[0]["section_start"]), int(front[0]["section_end"])))

    # Back nine: holes 10-18
    if max_hole >= 10:
        back = (
            course_df.filter(
                (F.col("hole_number") >= 10) & (F.col("hole_number") <= 18)
            )
            .agg(
                F.min("section_number").alias("section_start"),
                F.max("section_number").alias("section_end"),
            )
            .collect()
        )
        if back and back[0]["section_start"] is not None:
            nines.append(
                (2, int(back[0]["section_start"]), int(back[0]["section_end"]))
            )

    # Third nine: holes 19-27 (rare but possible)
    if max_hole >= 19:
        third = (
            course_df.filter(
                (F.col("hole_number") >= 19) & (F.col("hole_number") <= 27)
            )
            .agg(
                F.min("section_number").alias("section_start"),
                F.max("section_number").alias("section_end"),
            )
            .collect()
        )
        if third and third[0]["section_start"] is not None:
            nines.append(
                (3, int(third[0]["section_start"]), int(third[0]["section_end"]))
            )

    return nines


def infer_nine_boundaries_from_sections(
    course_df: DataFrame, max_section: int
) -> List[Tuple[int, int, int]]:
    """Infer nine boundaries for courses where hole_number resets 1-9 per nine."""
    # Group by section_number and find the dominant hole_number for each section
    section_holes = (
        course_df.groupBy("section_number")
        .agg(
            F.mode("hole_number").alias("dominant_hole"),
            F.count("*").alias("fix_count"),
        )
        .filter(F.col("fix_count") >= 5)
    )  # Filter noise

    section_data = section_holes.orderBy("section_number").collect()
    if not section_data:
        return [(1, 1, max_section)]

    # Find boundaries where hole resets (e.g., hole 9 -> hole 1)
    boundaries = [int(section_data[0]["section_number"])]
    prev_hole: Optional[int] = None

    for row in section_data:
        current_hole = row["dominant_hole"]
        current_section = int(row["section_number"])
        if prev_hole is not None and current_hole is not None:
            # Detect reset: high hole followed by low hole
            if int(prev_hole) >= 7 and int(current_hole) <= 3:
                boundaries.append(current_section)
        prev_hole = int(current_hole) if current_hole is not None else None

    # Add end boundary
    last_section = int(section_data[-1]["section_number"])
    boundaries.append(last_section + 1)

    # Convert boundaries to nine ranges
    nines: List[Tuple[int, int, int]] = []
    boundaries = sorted(set(boundaries))
    for i in range(len(boundaries) - 1):
        section_start = boundaries[i]
        section_end = boundaries[i + 1] - 1
        if section_end >= section_start:
            nine_number = i + 1
            nines.append((nine_number, section_start, section_end))

    # Limit to 4 nines max (handles edge cases)
    return nines[:4]


def infer_topology_for_course(course_id: str, course_df: DataFrame) -> CourseTopology:
    """Infer complete topology for a single course."""
    stats = course_df.agg(
        F.max("hole_number").alias("max_hole"),
        F.max("section_number").alias("max_section"),
        F.min("section_number").alias("min_section"),
    ).collect()[0]

    max_hole = int(stats["max_hole"] or 0)
    max_section = int(stats["max_section"] or 0)
    min_section = int(stats["min_section"] or 1)

    course_type = infer_course_type(max_hole, max_section)

    if course_type == "18-hole":
        nines = infer_nine_boundaries_from_holes(course_df, max_hole)
    elif course_type in {"27-hole", "18-hole-loop"}:
        nines = infer_nine_boundaries_from_sections(course_df, max_section)
        if course_type == "18-hole-loop" and len(nines) == 1:
            course_type = "9-hole"
    else:
        nines = [(1, min_section, max_section)]

    if not nines:
        nines = [(1, min_section, max_section)]

    return CourseTopology(course_id=course_id, course_type=course_type, nines=nines)


def generate_unit_name(course_type: str, nine_number: int, total_nines: int) -> str:
    """Generate a descriptive name for a nine/unit."""
    if total_nines == 1:
        return "Course"
    if total_nines == 2:
        return "Front Nine" if nine_number == 1 else "Back Nine"
    if total_nines == 3:
        names = {1: "Front Nine", 2: "Middle Nine", 3: "Back Nine"}
        return names.get(nine_number, f"Nine {nine_number}")
    return f"Nine {nine_number}"


def _topology_rows_to_csv(rows: Iterable[dict], output_csv: str) -> None:
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "facility_id",
        "unit_id",
        "unit_name",
        "nine_number",
        "section_start",
        "section_end",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})


def _create_topology_dataframe(
    spark: SparkSession, topology_rows: List[dict]
) -> DataFrame:
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("facility_id", StringType(), False),
            StructField("unit_id", IntegerType(), False),
            StructField("unit_name", StringType(), True),
            StructField("nine_number", IntegerType(), True),
            StructField("section_start", IntegerType(), True),
            StructField("section_end", IntegerType(), True),
        ]
    )

    return spark.createDataFrame(topology_rows, schema=schema)


def build_topology_rows(
    spark: SparkSession,
    min_fixes: int = 100,
    verbose: bool = True,
) -> List[dict]:
    """Infer topology for all courses that have sufficient Silver telemetry data."""
    telemetry_df = spark.table("iceberg.silver.fact_telemetry_event")

    valid_df = telemetry_df.filter(
        (F.col("is_location_padding") == False)
        & F.col("hole_number").isNotNull()
        & F.col("section_number").isNotNull()
    ).select("course_id", "hole_number", "section_number")

    course_counts = valid_df.groupBy("course_id").count()
    courses_to_process = (
        course_counts.filter(F.col("count") >= int(min_fixes))
        .select("course_id")
        .orderBy("course_id")
        .collect()
    )

    if verbose:
        print(f"Found {len(courses_to_process)} courses with >= {min_fixes} fixes")

    topology_rows: List[dict] = []
    for row in courses_to_process:
        course_id = row["course_id"]
        course_df = valid_df.filter(F.col("course_id") == course_id)
        topology = infer_topology_for_course(course_id, course_df)

        if verbose:
            print(
                f"  {course_id}: {topology.course_type} ({len(topology.nines)} nine(s))"
            )

        for nine_num, start, end in topology.nines:
            unit_name = generate_unit_name(
                topology.course_type, nine_num, len(topology.nines)
            )
            topology_rows.append(
                {
                    "facility_id": course_id,
                    "unit_id": int(nine_num),
                    "unit_name": unit_name,
                    "nine_number": int(nine_num),
                    "section_start": int(start),
                    "section_end": int(end),
                }
            )

    return topology_rows


def merge_topology_table(
    spark: SparkSession,
    df: DataFrame,
    target_table: str = "iceberg.silver.dim_facility_topology",
    preserve_existing_unit_names: bool = False,
) -> int:
    """Upsert topology rows into an Iceberg table."""
    # Ensure schema exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    # Ensure table exists (and evolve schema if it already exists)
    table_exists = True
    try:
        spark.sql(f"DESCRIBE TABLE {target_table}")
    except Exception:
        table_exists = False

    if not table_exists:
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
              facility_id STRING,
              unit_id INT,
              unit_name STRING,
              nine_number INT,
              section_start INT,
              section_end INT,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            """
        )
    else:
        # Some earlier local runs created the table without audit columns; add them if missing.
        try:
            existing_cols = {f.name for f in spark.table(target_table).schema.fields}
            if "created_at" not in existing_cols:
                spark.sql(
                    f"ALTER TABLE {target_table} ADD COLUMN created_at TIMESTAMP"
                )
            if "updated_at" not in existing_cols:
                spark.sql(
                    f"ALTER TABLE {target_table} ADD COLUMN updated_at TIMESTAMP"
                )
        except Exception:
            # If schema evolution isn't supported in a given environment, MERGE will fail loudly
            # and the operator can recreate the table.
            pass

    # Normalize empty strings to NULLs
    staged = (
        df.withColumn(
            "unit_name",
            F.when(F.col("unit_name") == "", F.lit(None)).otherwise(F.col("unit_name")),
        )
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
    staged.createOrReplaceTempView("topology_incoming")

    if preserve_existing_unit_names:
        # Only fill unit_name when target is NULL/empty.
        unit_name_update_expr = (
            "unit_name = CASE "
            "WHEN (t.unit_name IS NULL OR t.unit_name = '') AND (s.unit_name IS NOT NULL AND s.unit_name <> '') "
            "THEN s.unit_name ELSE t.unit_name END"
        )
    else:
        unit_name_update_expr = "unit_name = s.unit_name"

    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING topology_incoming s
        ON t.facility_id = s.facility_id
           AND t.unit_id = s.unit_id
           AND t.nine_number = s.nine_number
           AND t.section_start = s.section_start
           AND t.section_end = s.section_end
        WHEN MATCHED THEN UPDATE SET
          {unit_name_update_expr},
          updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
          facility_id,
          unit_id,
          unit_name,
          nine_number,
          section_start,
          section_end,
          created_at,
          updated_at
        ) VALUES (
          s.facility_id,
          s.unit_id,
          s.unit_name,
          s.nine_number,
          s.section_start,
          s.section_end,
          s.created_at,
          s.updated_at
        )
        """
    )

    return spark.table(target_table).count()


def seed_topology_from_s3(
    spark: SparkSession,
    csv_path: str,
    target_table: str = "iceberg.silver.dim_facility_topology",
) -> int:
    """Seed topology table from a CSV stored in S3/MinIO (s3a://...)."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("facility_id", StringType(), True),
            StructField("unit_id", IntegerType(), True),
            StructField("unit_name", StringType(), True),
            StructField("nine_number", IntegerType(), True),
            StructField("section_start", IntegerType(), True),
            StructField("section_end", IntegerType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(csv_path)
    return merge_topology_table(
        spark,
        df=df,
        target_table=target_table,
        preserve_existing_unit_names=False,  # curated CSV should overwrite
    )


def seed_course_profile_from_s3(
    spark: SparkSession,
    csv_path: str,
    target_table: str = "iceberg.silver.dim_course_profile",
) -> int:
    """Seed course profile table from a CSV stored in S3/MinIO (s3a://...)."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("course_id", StringType(), False),
            StructField("course_type", StringType(), True),
            StructField("volume_profile", StringType(), True),
            StructField("peak_season_start_month", IntegerType(), True),
            StructField("peak_season_end_month", IntegerType(), True),
            StructField("notes", StringType(), True),
            StructField("source", StringType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(csv_path)

    # Normalize empties to NULLs (avoid F.nullif for compatibility)
    for c in ["course_type", "volume_profile", "notes", "source"]:
        df = df.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
          course_id STRING,
          course_type STRING,
          volume_profile STRING,
          peak_season_start_month INT,
          peak_season_end_month INT,
          notes STRING,
          source STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        )
        USING iceberg
        """
    )

    staged = df.withColumn("created_at", F.current_timestamp()).withColumn(
        "updated_at", F.current_timestamp()
    )
    staged.createOrReplaceTempView("course_profile_incoming")

    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING course_profile_incoming s
        ON t.course_id = s.course_id
        WHEN MATCHED THEN UPDATE SET
          course_type = s.course_type,
          volume_profile = s.volume_profile,
          peak_season_start_month = s.peak_season_start_month,
          peak_season_end_month = s.peak_season_end_month,
          notes = s.notes,
          source = s.source,
          updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
          course_id,
          course_type,
          volume_profile,
          peak_season_start_month,
          peak_season_end_month,
          notes,
          source,
          created_at,
          updated_at
        ) VALUES (
          s.course_id,
          s.course_type,
          s.volume_profile,
          s.peak_season_start_month,
          s.peak_season_end_month,
          s.notes,
          s.source,
          s.created_at,
          s.updated_at
        )
        """
    )

    return spark.table(target_table).count()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TagMarshal dimension utilities")
    sub = parser.add_subparsers(dest="cmd", required=True)

    gen = sub.add_parser(
        "generate-topology",
        help="Infer topology from Silver telemetry; write CSV and/or upsert an Iceberg table",
    )
    gen.add_argument(
        "--min-fixes", type=int, default=100, help="Minimum fixes per course"
    )
    gen.add_argument(
        "--output-csv",
        default=None,
        help="Write inferred topology to a local CSV path (inside the container)",
    )
    # Back-compat alias (older Justfile used --output)
    gen.add_argument(
        "--output",
        default=None,
        help="DEPRECATED alias for --output-csv (kept for compatibility)",
    )
    gen.add_argument(
        "--output-table",
        default=None,
        help="Upsert inferred topology into this Iceberg table (e.g. iceberg.silver.dim_facility_topology)",
    )
    gen.add_argument(
        "--preserve-unit-names",
        action="store_true",
        help="When upserting to a table, do not overwrite existing non-empty unit_name values",
    )

    seed_top = sub.add_parser(
        "seed-topology",
        help="Seed dim_facility_topology from a CSV in S3/MinIO (s3a://...)",
    )
    seed_top.add_argument(
        "--csv",
        default=None,
        help="CSV path (s3a://...). Default uses bucket_source/seeds/dim_facility_topology.csv",
    )
    seed_top.add_argument(
        "--target-table",
        default="iceberg.silver.dim_facility_topology",
        help="Iceberg target table",
    )

    seed_prof = sub.add_parser(
        "seed-course-profile",
        help="Seed dim_course_profile from a CSV in S3/MinIO (s3a://...)",
    )
    seed_prof.add_argument(
        "--csv",
        default=None,
        help="CSV path (s3a://...). Default uses bucket_source/seeds/dim_course_profile.csv",
    )
    seed_prof.add_argument(
        "--target-table",
        default="iceberg.silver.dim_course_profile",
        help="Iceberg target table",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    cfg = TMConfig.from_env()
    spark = create_iceberg_spark_session(app_name=f"Dimensions::{args.cmd}", config=cfg)

    try:
        if args.cmd == "generate-topology":
            output_csv = args.output_csv or args.output  # back-compat
            rows = build_topology_rows(spark, min_fixes=args.min_fixes, verbose=True)

            if output_csv:
                _topology_rows_to_csv(rows, output_csv)
                print(f"✅ Topology CSV written: {output_csv}")

            if args.output_table:
                df = _create_topology_dataframe(spark, rows)
                total = merge_topology_table(
                    spark,
                    df=df,
                    target_table=args.output_table,
                    preserve_existing_unit_names=bool(args.preserve_unit_names),
                )
                print(f"✅ Topology table ready: {total} rows in {args.output_table}")

            if not output_csv and not args.output_table:
                raise ValueError(
                    "Nothing to do: provide --output-csv/--output or --output-table"
                )

            return 0

        if args.cmd == "seed-topology":
            csv_path = (
                args.csv or f"s3a://{cfg.bucket_source}/seeds/dim_facility_topology.csv"
            )
            total = seed_topology_from_s3(
                spark, csv_path=csv_path, target_table=args.target_table
            )
            print(
                f"✅ dim_facility_topology ready: {total} rows in {args.target_table}"
            )
            return 0

        if args.cmd == "seed-course-profile":
            csv_path = (
                args.csv or f"s3a://{cfg.bucket_source}/seeds/dim_course_profile.csv"
            )
            total = seed_course_profile_from_s3(
                spark, csv_path=csv_path, target_table=args.target_table
            )
            print(f"✅ dim_course_profile ready: {total} rows in {args.target_table}")
            return 0

        raise ValueError(f"Unknown command: {args.cmd}")
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
