import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def main():
    print("Seeding dim_facility_topology Iceberg table...")

    # Initialize Spark with Iceberg config
    # We need to manually add the JARs to the classpath if running via python directly
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

    # Set AWS Region explicitly
    os.environ["AWS_REGION"] = os.environ.get("TM_S3_REGION", "us-east-1")
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("TM_S3_ACCESS_KEY", "minioadmin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get(
        "TM_S3_SECRET_KEY", "minioadmin"
    )
    # Also pass region via Spark config so the JVM-side AWS SDK can resolve it reliably
    region = os.environ.get("TM_S3_REGION", os.environ.get("AWS_REGION", "us-east-1"))

    spark = (
        SparkSession.builder.appName("SeedTopology")
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
        .config("spark.sql.catalog.iceberg.s3.region", region)
        # Hadoop S3A configuration for reading CSV directly via s3a://
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.environ.get("TM_S3_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.environ.get("TM_S3_ACCESS_KEY", "minioadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.environ.get("TM_S3_SECRET_KEY", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.defaultCatalog", "iceberg")
        .getOrCreate()
    )

    # Define schema explicitly to ensure types match Trino definition
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

    # Read the CSV from MinIO (we uploaded it to seeds/)
    # Note: Spark reads s3a://...
    csv_path = "s3a://tm-lakehouse-source-store/seeds/dim_facility_topology.csv"

    try:
        df = spark.read.option("header", "true").schema(schema).csv(csv_path)
        count = df.count()
        print(f"Read {count} rows from {csv_path}")

        target_table = "iceberg.silver.dim_facility_topology"
        # Create table if missing, then MERGE (idempotent; avoids createOrReplace locking behavior)
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.silver.dim_facility_topology (
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

        # Normalize empty strings to NULLs
        df = df.withColumn(
            "unit_name", F.when(F.col("unit_name") == "", F.lit(None)).otherwise(F.col("unit_name"))
        )

        staged = df.withColumn("created_at", F.current_timestamp()).withColumn(
            "updated_at", F.current_timestamp()
        )
        staged.createOrReplaceTempView("topology_incoming")

        spark.sql(
            """
            MERGE INTO iceberg.silver.dim_facility_topology t
            USING topology_incoming s
            ON t.facility_id = s.facility_id
               AND t.unit_id = s.unit_id
               AND t.nine_number = s.nine_number
               AND t.section_start = s.section_start
               AND t.section_end = s.section_end
            WHEN MATCHED THEN UPDATE SET
              unit_name = s.unit_name,
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

        final_count = spark.table(target_table).count()
        print(f"✅ dim_facility_topology ready: {final_count} rows in {target_table}")

    except Exception as e:
        print(f"❌ Error seeding table: {e}")
        raise


if __name__ == "__main__":
    main()
