import os
import sys
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():
    parser = argparse.ArgumentParser(
        description="Generate topology CSV from Silver data"
    )
    parser.add_argument("--output", help="Output path for the CSV file", default=None)
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

    # 1. Calculate the max section for each course to determine its size
    course_stats = df.groupBy("course_id").agg(
        F.max("section_number").alias("max_section"),
        F.max("hole_number").alias("max_hole"),
    )

    # 2. Collect results to driver (metadata is small, so this is safe)
    courses = course_stats.collect()

    topology_rows = []

    # 3. Generate Topology Rules based on data shape
    print(f"Analyzing {len(courses)} courses...")

    for row in courses:
        cid = row.course_id
        max_sec = row.max_section if row.max_section else 0

        # Heuristic: 27 sections per 9 holes (3 sections per hole: Tee, Fairway, Green)
        # We generate a row for every 27-section block we find

        # Nine 1 (Sections 1-27)
        if max_sec >= 1:
            topology_rows.append(f"{cid},1,Front / Unit 1,1,1,27")

        # Nine 2 (Sections 28-54)
        if max_sec >= 28:
            topology_rows.append(f"{cid},2,Back / Unit 2,2,28,54")

        # Nine 3 (Sections 55-81) - The "27 Hole" case
        if max_sec >= 55:
            topology_rows.append(f"{cid},3,Third / Unit 3,3,55,81")

        # Nine 4 (Sections 82+) - Rare 36 hole processing
        if max_sec >= 82:
            topology_rows.append(f"{cid},4,Fourth / Unit 4,4,82,999")

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

    print(f"✅ Generated topology for {len(courses)} courses at: {output_path}")
    print(
        "You can now manually edit 'unit_name' in this file for specific courses (e.g. rename 'Unit 1' to 'Red')."
    )


if __name__ == "__main__":
    main()
