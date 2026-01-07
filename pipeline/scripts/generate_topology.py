"""Generate topology CSV from Silver telemetry data.

This script analyzes the Silver layer data to determine course topology:
- How many nines a course has (1, 2, or 3)
- What section ranges correspond to each nine
- Whether the course uses holes 1-18 or resets holes 1-9 per nine

Course Type Detection Logic (from raw data analysis):
- 18-hole courses: hole_number goes 1-18, sections roughly 1-54
- 27-hole courses: hole_number resets 1-9 per nine, sections go 1-81
- 9-hole courses: hole_number goes 1-9, sections roughly 1-24
"""

import os
import sys
import argparse
from typing import List, Dict
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Ensure shared lib is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from tm_lakehouse.config import TMConfig
from tm_lakehouse.spark_utils import create_iceberg_spark_session


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class CourseMetrics:
    """Metrics extracted from course telemetry data."""

    course_id: str
    max_hole_number: int
    max_section_number: int
    min_section_number: int
    unique_holes: int


@dataclass
class NineBoundary:
    """Boundary definition for a single nine."""

    nine_number: int
    section_start: int
    section_end: int
    unit_name: str


# =============================================================================
# COURSE TYPE DETECTION
# =============================================================================


def determine_course_type(max_hole_number: int, max_section_number: int) -> str:
    """Determine course type based on hole and section ranges.

    Args:
        max_hole_number: Highest hole number observed in the data
        max_section_number: Highest section number observed in the data

    Returns:
        Course type string: "18-hole", "27-hole", "9-hole-loop", or "9-hole"
    """
    if max_hole_number >= 10:
        # Holes go 1-18, this is a standard 18-hole course
        return "18-hole"
    elif max_section_number > 54:
        # Holes reset 1-9 but sections go beyond 54, this is a 27-hole course
        return "27-hole"
    elif max_section_number > 27:
        # Holes 1-9, sections 28-54 range - could be 9-hole playing loops
        return "9-hole-loop"
    else:
        # Simple 9-hole course
        return "9-hole"


# =============================================================================
# NINE BOUNDARY CALCULATION
# =============================================================================


def calculate_nine_boundaries_for_18_hole(
    course_id: str, hole_section_df: DataFrame
) -> List[NineBoundary]:
    """Calculate nine boundaries for standard 18-hole courses.

    For 18-hole courses, holes 1-9 are the front nine and holes 10-18 are the back nine.
    We find the actual section ranges where these holes appear.

    Args:
        course_id: The course identifier
        hole_section_df: DataFrame with course_id, hole_number, section_number columns

    Returns:
        List of NineBoundary objects defining front and back nine
    """
    course_data = hole_section_df.filter(F.col("course_id") == course_id)

    boundaries = []

    # Front nine: holes 1-9
    front_nine = (
        course_data.filter((F.col("hole_number") >= 1) & (F.col("hole_number") <= 9))
        .agg(
            F.min("section_number").alias("section_start"),
            F.max("section_number").alias("section_end"),
        )
        .collect()
    )

    if front_nine and front_nine[0]["section_start"] is not None:
        boundaries.append(
            NineBoundary(
                nine_number=1,
                section_start=int(front_nine[0]["section_start"]),
                section_end=int(front_nine[0]["section_end"]),
                unit_name="Front Nine",
            )
        )

    # Back nine: holes 10-18
    back_nine = (
        course_data.filter((F.col("hole_number") >= 10) & (F.col("hole_number") <= 18))
        .agg(
            F.min("section_number").alias("section_start"),
            F.max("section_number").alias("section_end"),
        )
        .collect()
    )

    if back_nine and back_nine[0]["section_start"] is not None:
        boundaries.append(
            NineBoundary(
                nine_number=2,
                section_start=int(back_nine[0]["section_start"]),
                section_end=int(back_nine[0]["section_end"]),
                unit_name="Back Nine",
            )
        )

    return boundaries


def calculate_nine_boundaries_for_27_hole(
    course_id: str, hole_section_df: DataFrame, max_section: int
) -> List[NineBoundary]:
    """Calculate nine boundaries for 27-hole courses where holes reset 1-9.

    For 27-hole courses, hole numbers reset 1-9 for each nine, but section numbers
    are cumulative (1-27 for nine 1, 28-54 for nine 2, 55-81 for nine 3).

    Args:
        course_id: The course identifier
        hole_section_df: DataFrame with course_id, hole_number, section_number columns
        max_section: Maximum section number observed

    Returns:
        List of NineBoundary objects defining up to 3 nines
    """
    # Standard section boundaries for 27-hole courses
    # Each nine has approximately 27 sections (9 holes * 3 sections per hole)
    nine_definitions = [
        (1, 1, 27, "Front Nine"),
        (2, 28, 54, "Middle Nine"),
        (3, 55, 81, "Back Nine"),
    ]

    course_data = hole_section_df.filter(F.col("course_id") == course_id)
    boundaries = []

    for nine_num, expected_start, expected_end, name in nine_definitions:
        # Check if this nine has data
        nine_data = (
            course_data.filter(
                (F.col("section_number") >= expected_start)
                & (F.col("section_number") <= expected_end)
            )
            .agg(
                F.min("section_number").alias("actual_start"),
                F.max("section_number").alias("actual_end"),
                F.count("*").alias("fix_count"),
            )
            .collect()
        )

        if nine_data and nine_data[0]["fix_count"] and nine_data[0]["fix_count"] > 0:
            actual_start = int(nine_data[0]["actual_start"])
            actual_end = int(nine_data[0]["actual_end"])

            boundaries.append(
                NineBoundary(
                    nine_number=nine_num,
                    section_start=actual_start,
                    section_end=actual_end,
                    unit_name=name,
                )
            )

    return boundaries


def calculate_nine_boundaries_for_9_hole(
    course_id: str, hole_section_df: DataFrame
) -> List[NineBoundary]:
    """Calculate nine boundaries for 9-hole courses.

    For 9-hole courses (including loop courses), there's just one nine.
    The section range is determined from the data.

    Args:
        course_id: The course identifier
        hole_section_df: DataFrame with course_id, hole_number, section_number columns

    Returns:
        List with single NineBoundary for the loop course
    """
    course_data = hole_section_df.filter(F.col("course_id") == course_id)

    stats = course_data.agg(
        F.min("section_number").alias("section_start"),
        F.max("section_number").alias("section_end"),
    ).collect()

    if stats and stats[0]["section_start"] is not None:
        return [
            NineBoundary(
                nine_number=1,
                section_start=int(stats[0]["section_start"]),
                section_end=int(stats[0]["section_end"]),
                unit_name="Loop Course",
            )
        ]

    return []


# =============================================================================
# SPARK SESSION
# =============================================================================

# Spark session creation is now handled by the shared spark_utils module.
# See create_iceberg_spark_session() in tm_lakehouse/spark_utils.py


# =============================================================================
# DATA EXTRACTION
# =============================================================================


def extract_course_metrics(telemetry_df: DataFrame) -> List[CourseMetrics]:
    """Extract key metrics for each course from telemetry data.

    Args:
        telemetry_df: Silver telemetry DataFrame

    Returns:
        List of CourseMetrics for each course
    """
    # Filter to valid rows only
    valid_df = telemetry_df.filter(
        F.col("course_id").isNotNull()
        & F.col("hole_number").isNotNull()
        & F.col("section_number").isNotNull()
    )

    # Aggregate per course
    course_stats = (
        valid_df.groupBy("course_id")
        .agg(
            F.max("hole_number").alias("max_hole_number"),
            F.max("section_number").alias("max_section_number"),
            F.min("section_number").alias("min_section_number"),
            F.countDistinct("hole_number").alias("unique_holes"),
        )
        .collect()
    )

    return [
        CourseMetrics(
            course_id=row["course_id"],
            max_hole_number=int(row["max_hole_number"] or 0),
            max_section_number=int(row["max_section_number"] or 0),
            min_section_number=int(row["min_section_number"] or 0),
            unique_holes=int(row["unique_holes"] or 0),
        )
        for row in course_stats
    ]


def extract_hole_section_data(telemetry_df: DataFrame) -> DataFrame:
    """Extract hole and section data for boundary calculations.

    Args:
        telemetry_df: Silver telemetry DataFrame

    Returns:
        DataFrame with course_id, hole_number, section_number
    """
    return telemetry_df.filter(
        F.col("course_id").isNotNull()
        & F.col("hole_number").isNotNull()
        & F.col("section_number").isNotNull()
    ).select("course_id", "hole_number", "section_number")


# =============================================================================
# TOPOLOGY GENERATION
# =============================================================================


def generate_topology_for_course(
    metrics: CourseMetrics, hole_section_df: DataFrame
) -> List[NineBoundary]:
    """Generate topology boundaries for a single course.

    Args:
        metrics: CourseMetrics for the course
        hole_section_df: DataFrame with hole/section data

    Returns:
        List of NineBoundary objects for this course
    """
    course_type = determine_course_type(
        metrics.max_hole_number, metrics.max_section_number
    )

    print(
        f"  {metrics.course_id}: {course_type} (holes 1-{metrics.max_hole_number}, sections 1-{metrics.max_section_number})"
    )

    if course_type == "18-hole":
        return calculate_nine_boundaries_for_18_hole(metrics.course_id, hole_section_df)
    elif course_type == "27-hole":
        return calculate_nine_boundaries_for_27_hole(
            metrics.course_id, hole_section_df, metrics.max_section_number
        )
    else:  # 9-hole or 9-hole-loop
        return calculate_nine_boundaries_for_9_hole(metrics.course_id, hole_section_df)


def write_topology_csv(
    topology_data: Dict[str, List[NineBoundary]], output_path: str
) -> None:
    """Write topology data to CSV file.

    Args:
        topology_data: Dictionary mapping course_id to list of NineBoundary
        output_path: Path to write the CSV file
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    header = "facility_id,unit_id,unit_name,nine_number,section_start,section_end"
    rows = []

    for course_id in sorted(topology_data.keys()):
        for boundary in topology_data[course_id]:
            rows.append(
                f"{course_id},{boundary.nine_number},{boundary.unit_name},"
                f"{boundary.nine_number},{boundary.section_start},{boundary.section_end}"
            )

    with open(output_path, "w") as f:
        f.write(header + "\n")
        f.write("\n".join(rows))
        if rows:
            f.write("\n")

    print(f"✅ Written topology for {len(topology_data)} courses to: {output_path}")


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point for topology generation."""
    parser = argparse.ArgumentParser(
        description="Generate topology CSV from Silver telemetry data"
    )
    parser.add_argument("--output", help="Output path for the CSV file", default=None)
    args = parser.parse_args()

    print("=" * 60)
    print("TOPOLOGY GENERATOR")
    print("=" * 60)
    print()

    # Create Spark session using centralized utility
    print("Creating Spark session...")
    cfg = TMConfig.from_env()
    spark = create_iceberg_spark_session(
        app_name="TopologyGenerator", config=cfg, log_level="ERROR"
    )

    # Read Silver data
    print("Reading Silver telemetry data...")
    try:
        telemetry_df = spark.table("iceberg.silver.fact_telemetry_event")
    except Exception as e:
        print(f"Error: Could not read Silver table: {e}")
        print("Make sure the Silver ETL has been run first.")
        spark.stop()
        return

    # Extract metrics and data
    print("Extracting course metrics...")
    course_metrics_list = extract_course_metrics(telemetry_df)
    hole_section_df = extract_hole_section_data(telemetry_df)

    print(f"Found {len(course_metrics_list)} courses")
    print()

    # Generate topology for each course
    print("Generating topology boundaries:")
    topology_data = {}

    for metrics in course_metrics_list:
        boundaries = generate_topology_for_course(metrics, hole_section_df)
        if boundaries:
            topology_data[metrics.course_id] = boundaries

    print()

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "silver/seeds/dim_facility_topology.csv",
        )

    # Write CSV
    write_topology_csv(topology_data, output_path)

    # Cleanup
    spark.stop()
    print("Done!")


if __name__ == "__main__":
    main()
