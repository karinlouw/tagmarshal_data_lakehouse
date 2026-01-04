# Custom Spark image with pre-baked dependencies for TagMarshal Lakehouse
# This avoids the ~10 minute download time and network reliability issues
FROM apache/spark:3.5.0

USER root

# Create directory for extra JARs
RUN mkdir -p /opt/spark/extra-jars

# Download required dependencies at build time (much more reliable)
# These are needed for S3/MinIO access and Iceberg tables
RUN curl -fSL -o /opt/spark/extra-jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o /opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL -o /opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
    curl -fSL -o /opt/spark/extra-jars/bundle-2.20.18.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar && \
    curl -fSL -o /opt/spark/extra-jars/wildfly-openssl-1.0.7.Final.jar \
    https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar && \
    curl -fSL -o /opt/spark/extra-jars/eventstream-1.0.1.jar \
    https://repo1.maven.org/maven2/software/amazon/eventstream/eventstream/1.0.1/eventstream-1.0.1.jar

# Verify downloads completed (check files are not empty)
RUN ls -la /opt/spark/extra-jars/ && \
    for jar in /opt/spark/extra-jars/*.jar; do \
      SIZE=$(stat -c%s "$jar" 2>/dev/null || stat -f%z "$jar"); \
      if [ "$SIZE" -lt 10000 ]; then \
        echo "ERROR: $jar is too small ($SIZE bytes)"; \
        exit 1; \
      fi; \
    done && echo "All JARs verified (total size: $(du -sh /opt/spark/extra-jars/))"

# Set permissions
RUN chmod -R 755 /opt/spark/extra-jars

USER spark

