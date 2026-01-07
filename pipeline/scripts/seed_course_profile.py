import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


def main():
    print("Seeding dim_course_profile Iceberg table...")

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

    existing_jars = [j for j in extra_jars if os.path.exists(j)]
    jars_conf = ",".join(existing_jars)

    # Set AWS Region explicitly (MinIO/S3A)
    os.environ["AWS_REGION"] = os.environ.get("TM_S3_REGION", "us-east-1")
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("TM_S3_ACCESS_KEY", "minioadmin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get(
        "TM_S3_SECRET_KEY", "minioadmin"
    )
    # Also pass region via Spark config so the JVM-side AWS SDK can resolve it reliably
    region = os.environ.get("TM_S3_REGION", os.environ.get("AWS_REGION", "us-east-1"))

    spark = (
        SparkSession.builder.appName("SeedCourseProfile")
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
        # Hadoop S3A configuration for reading CSV via s3a://
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

    csv_path = "s3a://tm-lakehouse-source-store/seeds/dim_course_profile.csv"
    df = spark.read.option("header", "true").schema(schema).csv(csv_path)

    # Normalize empties to NULLs (avoid F.nullif for compatibility)
    for c in ["course_type", "volume_profile", "notes", "source"]:
        df = df.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))

    target_table = "iceberg.silver.dim_course_profile"

    # Ensure table exists (Iceberg SQL DDL)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.dim_course_profile (
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

    # Merge (upsert) so we can safely re-run without dropping existing records.
    staged = df.withColumn("created_at", F.current_timestamp()).withColumn(
        "updated_at", F.current_timestamp()
    )
    staged.createOrReplaceTempView("course_profile_incoming")

    spark.sql(
        """
        MERGE INTO iceberg.silver.dim_course_profile t
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

    count = spark.table(target_table).count()
    print(f"✅ dim_course_profile ready: {count} rows in {target_table}")


if __name__ == "__main__":
    main()
