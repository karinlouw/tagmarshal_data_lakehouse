"""Generate sections-per-hole dimension table from Silver telemetry data.

This script creates a dimension table that maps which sections belong to which holes
for each course. This is useful for understanding course structure and validating data.

The table shows:
- course_id: Course identifier
- hole_number: Hole number (1-27)
- section_start: Minimum section number for this hole
- section_end: Maximum section number for this hole
- sections_count: Number of sections in this hole
"""

import os
import sys
import argparse
from pathlib import Path

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Ensure shared lib is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from tm_lakehouse.config import TMConfig
from tm_lakehouse.spark_utils import create_iceberg_spark_session


def generate_sections_per_hole(
    spark, input_table: str = "iceberg.silver.fact_telemetry_event"
) -> DataFrame:
    """Generate sections-per-hole dimension from telemetry data.

    Args:
        spark: SparkSession
        input_table: Source table name

    Returns:
        DataFrame with columns: course_id, hole_number, section_start, section_end, sections_count
    """
    print("Reading telemetry data...")
    telemetry_df = spark.table(input_table)

    # Filter to valid location data (not padding, has both hole and section)
    valid_df = telemetry_df.filter(
        (F.col("is_location_padding") == False)
        & F.col("hole_number").isNotNull()
        & F.col("section_number").isNotNull()
    )

    # Aggregate by course and hole to find section ranges
    sections_per_hole = (
        valid_df.groupBy("course_id", "hole_number")
        .agg(
            F.min("section_number").alias("section_start"),
            F.max("section_number").alias("section_end"),
            F.countDistinct("section_number").alias("sections_count"),
        )
        .orderBy("course_id", "hole_number")
    )

    return sections_per_hole


def main():
    parser = argparse.ArgumentParser(
        description="Generate sections-per-hole dimension table from Silver data"
    )
    parser.add_argument(
        "--input-table",
        default="iceberg.silver.fact_telemetry_event",
        help="Input table name (default: iceberg.silver.fact_telemetry_event)",
    )
    parser.add_argument(
        "--output-table",
        default="iceberg.silver.dim_sections_per_hole",
        help="Output table name (default: iceberg.silver.dim_sections_per_hole)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("SECTIONS PER HOLE GENERATOR")
    print("=" * 60)
    print()

    # Create Spark session
    cfg = TMConfig.from_env()
    spark = create_iceberg_spark_session(
        app_name="SectionsPerHoleGenerator", config=cfg, log_level="WARN"
    )

    try:
        # Generate the dimension table
        sections_df = generate_sections_per_hole(spark, args.input_table)

        # Show sample
        print("Sample data:")
        sections_df.show(20, truncate=False)

        # Get summary stats
        course_counts = sections_df.groupBy("course_id").count().orderBy("course_id")
        print("\nHoles per course:")
        course_counts.show(truncate=False)

        # Write to table
        print(f"\nWriting to {args.output_table}...")

        # Create namespace if needed
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

        # Drop and recreate to ensure clean state
        try:
            spark.sql(f"DROP TABLE IF EXISTS {args.output_table}")
        except Exception:
            pass

        sections_df.writeTo(args.output_table).using("iceberg").create()

        print(f"Done. Sections-per-hole dimension saved to {args.output_table}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        spark.stop()
        # Fail the process so callers (Airflow/just) can detect the failure.
        # Without this, Spark can exit 0 even though nothing was written.
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
