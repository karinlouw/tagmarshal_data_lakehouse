import os
import sys
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def main():
    parser = argparse.ArgumentParser(
        description="Generate topology CSV from Silver data"
    )
    parser.add_argument("--output", help="Output path for the CSV file", default=None)
    parser.add_argument(
        "--min-fixes-per-section",
        type=int,
        default=int(os.environ.get("TM_TOPOLOGY_MIN_FIXES_PER_SECTION", "25")),
        help="Minimum number of fixes required for a section_number to be considered reliable (filters GPS/geofence outliers).",
    )
    parser.add_argument(
        "--reliable-range-pad",
        type=int,
        default=int(os.environ.get("TM_TOPOLOGY_RELIABLE_RANGE_PAD", "2")),
        help="Allow small padding (in sections) outside the reliable min/max to avoid excluding legitimate edge sections.",
    )
    args = parser.parse_args()

    print("Starting Topology Discovery...")

    # Initialize Spark
    # We need to manually add the JARs to the classpath if running via python directly
    # and not spark-submit
    extra_jars = [
        "/opt/tagmarshal/pipeline/lib/iceberg-spark-runtime-3.5_2.12-1.4.3.jar",
        "/opt/tagmarshal/pipeline/lib/bundle-2.20.18.jar",
        "/opt/tagmarshal/pipeline/lib/url-connection-client-2.20.18.jar",
        "/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar",
        "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/extra-jars/bundle-2.20.18.jar",
        "/opt/spark/extra-jars/url-connection-client-2.20.18.jar",
    ]

    # Filter only existing jars
    existing_jars = [j for j in extra_jars if os.path.exists(j)]
    jars_conf = ",".join(existing_jars)

    print(f"Using JARs: {jars_conf}")

    # Set AWS Region explicitly to avoid SDK client exception
    os.environ["AWS_REGION"] = os.environ.get("TM_S3_REGION", "us-east-1")
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("TM_S3_ACCESS_KEY", "minioadmin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get(
        "TM_S3_SECRET_KEY", "minioadmin"
    )

    spark = (
        SparkSession.builder.appName("TopologyGenerator")
        .config("spark.jars", jars_conf)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config(
            "spark.sql.catalog.iceberg.warehouse",
            "s3://tm-lakehouse-source-store/warehouse",
        )
        .config(
            "spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(
            "spark.sql.catalog.iceberg.s3.endpoint",
            os.environ.get("TM_S3_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.sql.catalog.iceberg.s3.access-key-id",
            os.environ.get("TM_S3_ACCESS_KEY", "minioadmin"),
        )
        .config(
            "spark.sql.catalog.iceberg.s3.secret-access-key",
            os.environ.get("TM_S3_SECRET_KEY", "minioadmin"),
        )
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .getOrCreate()
    )

    # Read the existing Silver data
    try:
        df = spark.table("iceberg.silver.fact_telemetry_event")
    except Exception as e:
        print(f"Silver table not found or error accessing it: {e}")
        print("Run the ETL first with fallback logic to populate data.")
        return

    # Infer topology without assuming "3 sections per hole".
    #
    # We support two common encodings we see in the telemetry:
    #
    # A) hole_number spans 1..18 (or 1..27):
    #    - We can infer Front/Back/Third by hole_number bands (1-9, 10-18, 19-27...)
    #
    # B) hole_number resets 1..9 for each physical 9-hole unit (common on 27-hole facilities):
    #    - We infer unit boundaries by scanning section_number in order and detecting when the
    #      dominant hole_number drops (e.g., 9 -> 1). This avoids hardcoding 27-section blocks.

    df_valid = df.filter(
        F.col("course_id").isNotNull()
        & F.col("hole_number").isNotNull()
        & F.col("section_number").isNotNull()
    )

    # Frequency-aware filtering:
    # A single bad GPS/geofence classification can introduce rare "outlier" section_numbers.
    # We only treat section_numbers as topology-defining if they occur with sufficient frequency.
    min_fixes_per_section = int(args.min_fixes_per_section)
    reliable_range_pad = int(args.reliable_range_pad)

    # Count fixes per (course_id, hole_number, section_number)
    hole_section_counts = (
        df_valid.groupBy("course_id", "hole_number", "section_number")
        .count()
        .withColumnRenamed("count", "fixes")
    )

    # Filter to "reliable" section_numbers per hole
    hole_section_reliable = hole_section_counts.filter(
        F.col("fixes") >= F.lit(min_fixes_per_section)
    )

    # Per-hole section range (used for strategy A), computed from reliable section_numbers.
    # If a hole has no reliable sections (very low volume), we'll fall back to the unfiltered range.
    per_hole_reliable = hole_section_reliable.groupBy("course_id", "hole_number").agg(
        F.min("section_number").alias("min_section_number"),
        F.max("section_number").alias("max_section_number"),
    )

    per_hole_fallback = df_valid.groupBy("course_id", "hole_number").agg(
        F.min("section_number").alias("min_section_number_fallback"),
        F.max("section_number").alias("max_section_number_fallback"),
    )

    per_hole = (
        per_hole_fallback.join(
            per_hole_reliable, on=["course_id", "hole_number"], how="left"
        )
        .withColumn(
            "min_section_number",
            F.when(
                F.col("min_section_number").isNull(),
                F.col("min_section_number_fallback"),
            )
            # If fallback min is only slightly outside the reliable min (within pad),
            # keep it so we don't exclude legitimate edge sections (e.g., section 1).
            .when(
                F.col("min_section_number_fallback")
                >= (F.col("min_section_number") - F.lit(reliable_range_pad)),
                F.col("min_section_number_fallback"),
            ).otherwise(F.col("min_section_number")),
        )
        .withColumn(
            "max_section_number",
            F.when(
                F.col("max_section_number").isNull(),
                F.col("max_section_number_fallback"),
            )
            # If fallback max is only slightly outside the reliable max (within pad),
            # keep it; otherwise clamp to the reliable max to avoid outliers.
            .when(
                F.col("max_section_number_fallback")
                <= (F.col("max_section_number") + F.lit(reliable_range_pad)),
                F.col("max_section_number_fallback"),
            ).otherwise(F.col("max_section_number")),
        )
        .select("course_id", "hole_number", "min_section_number", "max_section_number")
    )

    # Reliable section_numbers at the course level (used for strategy B endpoint clamping)
    course_section_counts = (
        df_valid.groupBy("course_id", "section_number")
        .count()
        .withColumnRenamed("count", "fixes")
    )
    course_section_reliable = course_section_counts.filter(
        F.col("fixes") >= F.lit(min_fixes_per_section)
    )

    # Per-course metadata (small)
    course_meta = (
        df_valid.groupBy("course_id")
        .agg(F.max("hole_number").alias("max_hole_number_observed"))
        .collect()
    )

    topology_rows = []
    print(f"Analyzing {len(course_meta)} courses...")

    hole_ranges = [
        (1, 1, 9, "Front / Unit 1"),
        (2, 10, 18, "Back / Unit 2"),
        (3, 19, 27, "Third / Unit 3"),
        (4, 28, 36, "Fourth / Unit 4"),
    ]

    for row in course_meta:
        cid = row.course_id
        max_hole = int(row.max_hole_number_observed or 0)

        if max_hole >= 10:
            # Strategy A: hole_number bands
            holes_df = per_hole.filter(F.col("course_id") == cid)

            for unit_id, h_start, h_end, unit_name in hole_ranges:
                if max_hole < h_start:
                    continue

                band = holes_df.filter(
                    (F.col("hole_number") >= h_start) & (F.col("hole_number") <= h_end)
                )
                stats = band.agg(
                    F.min("min_section_number").alias("section_start"),
                    F.max("max_section_number").alias("section_end"),
                ).collect()[0]

                section_start = stats["section_start"]
                section_end = stats["section_end"]
                if section_start is None or section_end is None:
                    continue

                nine_number = unit_id
                topology_rows.append(
                    f"{cid},{unit_id},{unit_name},{nine_number},{int(section_start)},{int(section_end)}"
                )
        else:
            # Strategy B: detect hole_number resets while section_number increases
            # Only consider reliable section_numbers to avoid outlier-driven boundaries
            reliable_sections_for_course = course_section_reliable.filter(
                F.col("course_id") == cid
            ).select("section_number")
            counts = (
                df_valid.filter(F.col("course_id") == cid)
                .join(reliable_sections_for_course, on="section_number", how="inner")
                .groupBy("section_number", "hole_number")
                .count()
            )
            w = Window.partitionBy("section_number").orderBy(F.col("count").desc())
            dominant = (
                counts.withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .select(
                    "section_number", F.col("hole_number").alias("dominant_hole_number")
                )
                .orderBy("section_number")
                .collect()
            )

            if not dominant:
                continue

            # Find boundaries where dominant hole resets (e.g. 9 -> 1)
            boundaries = [dominant[0]["section_number"]]
            prev_hole = None
            for r in dominant:
                sec = int(r["section_number"])
                hole = int(r["dominant_hole_number"])
                if prev_hole is not None:
                    # Reset heuristic: we were near end of a nine and we jumped back to early holes
                    if prev_hole >= 8 and hole <= 2:
                        boundaries.append(sec)
                prev_hole = hole

            # Always close with an end marker (last section + 1)
            boundaries = sorted(set(boundaries))
            # Clamp end using reliable section range (avoid rare max section outlier)
            last_section_row = (
                course_section_reliable.filter(F.col("course_id") == cid)
                .agg(
                    F.min("section_number").alias("min_reliable_section"),
                    F.max("section_number").alias("max_reliable_section"),
                )
                .collect()[0]
            )
            max_reliable_section = last_section_row["max_reliable_section"]
            min_reliable_section = last_section_row["min_reliable_section"]

            # Also allow small padding around the reliable range to avoid excluding legitimate edges
            course_minmax_fallback = (
                df_valid.filter(F.col("course_id") == cid)
                .agg(
                    F.min("section_number").alias("min_section_fallback"),
                    F.max("section_number").alias("max_section_fallback"),
                )
                .collect()[0]
            )
            min_section_fallback = course_minmax_fallback["min_section_fallback"]
            max_section_fallback = course_minmax_fallback["max_section_fallback"]

            # Start boundary: use fallback min if it's within pad of reliable min; else use reliable min.
            if min_reliable_section is not None:
                if (
                    min_section_fallback is not None
                    and int(min_section_fallback)
                    >= int(min_reliable_section) - reliable_range_pad
                ):
                    boundaries[0] = int(min_section_fallback)
                else:
                    boundaries[0] = int(min_reliable_section)

            # End boundary: use fallback max if it's within pad of reliable max; else clamp to reliable max.
            if max_reliable_section is not None:
                if (
                    max_section_fallback is not None
                    and int(max_section_fallback)
                    <= int(max_reliable_section) + reliable_range_pad
                ):
                    last_section = int(max_section_fallback)
                else:
                    last_section = int(max_reliable_section)
            else:
                last_section = int(dominant[-1]["section_number"])
            boundaries.append(last_section + 1)

            # Build segments
            segments = []
            for idx in range(len(boundaries) - 1):
                section_start = boundaries[idx]
                section_end = boundaries[idx + 1] - 1
                if section_end >= section_start:
                    segments.append((int(section_start), int(section_end)))

            # If we detect a tiny trailing segment, it's usually noise (e.g. one-off section),
            # so merge it into the previous segment to avoid creating a spurious "Unit 4".
            if len(segments) >= 2:
                last_len = segments[-1][1] - segments[-1][0] + 1
                if last_len < 5:
                    prev = segments[-2]
                    segments[-2] = (prev[0], segments[-1][1])
                    segments = segments[:-1]

            for idx, (section_start, section_end) in enumerate(segments):
                unit_id = idx + 1
                if unit_id > 4:
                    break
                unit_name = f"Unit {unit_id}"
                nine_number = unit_id
                topology_rows.append(
                    f"{cid},{unit_id},{unit_name},{nine_number},{section_start},{section_end}"
                )

    # 4. Write to CSV
    header = "facility_id,unit_id,unit_name,nine_number,section_start,section_end"

    if args.output:
        output_path = args.output
    else:
        # Default path relative to script location (works if running locally or writable mount)
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "silver/seeds/dim_facility_topology.csv",
        )

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Read existing manual overrides if they exist (to preserve custom names)
    # For now, we just overwrite to demonstrate the automation, but in prod we'd merge
    with open(output_path, "w") as f:
        f.write(header + "\n")
        f.write("\n".join(topology_rows))

    print(f"✅ Generated topology for {len(course_meta)} courses at: {output_path}")
    print(
        "You can now manually edit 'unit_name' in this file for specific courses (e.g. rename 'Unit 1' to 'Red')."
    )


if __name__ == "__main__":
    main()
