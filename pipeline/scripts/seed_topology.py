import os
from pyspark.sql import SparkSession
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

        # Write to Iceberg table (overwrite to keep it fresh)
        # We use the namespace 'silver' which maps to tm-lakehouse-source-store/warehouse/silver
        target_table = "iceberg.silver.dim_facility_topology"

        df.writeTo(target_table).createOrReplace()
        print(f"✅ Successfully wrote to {target_table}")

    except Exception as e:
        print(f"❌ Error seeding table: {e}")
        raise


if __name__ == "__main__":
    main()
