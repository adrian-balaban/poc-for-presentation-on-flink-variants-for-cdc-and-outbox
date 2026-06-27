FROM docker.io/library/flink:2.2.1

USER root

# Increase JVM metaspace to prevent OOM during job submissions
ENV FLINK_JVM_OPTIONS="-XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m"

# Add the MySQL JDBC driver, Prometheus metrics reporter, and S3 filesystem plugin (presto).
# All are downloaded from Maven Central with pinned SHA-512 digests.
# On a version bump, regenerate the digests with: sha512sum <downloaded-jar>
RUN set -eux; \
    MYSQL_URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"; \
    PROM_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/2.2.1/flink-metrics-prometheus-2.2.1.jar"; \
    S3_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/2.2.1/flink-s3-fs-presto-2.2.1.jar"; \
    MYSQL_JAR=/opt/flink/lib/mysql-connector-j-8.0.33.jar; \
    PROM_JAR=/opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-2.2.1.jar; \
    S3_JAR=/opt/flink/plugins/s3-fs-presto/flink-s3-fs-presto-2.2.1.jar; \
    MYSQL_SHA512="dfc56aa76a7419a8ed2c0d051ed279ded8c9aff354018aac2f8c97b68a220784abe7a8ba4fc47e932c41bff61cb9637024a7e19cfaa37efd78cd8a3227f7baaa"; \
    PROM_SHA512="73cc78f88512afefb884a7705ad2aea23d8666984280f9350e2ecd3fa4af02e11c1c22a4afbec768717f2abff61844cb62b95e7c41a413f4f1b9abc6b72353fb"; \
    S3_SHA512="697e62205d584b43f29f5c652df6f6482bc3d80363a871b4b853433febf45af719a2e292e78d3bcd10e12dc1b358e792d0df322efc4f248ac739d0b8d8fe7a72"; \
    mkdir -p /opt/flink/plugins/metrics-prometheus /opt/flink/plugins/s3-fs-presto; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$MYSQL_URL" -o "$MYSQL_JAR"; \
    echo "$MYSQL_SHA512  $MYSQL_JAR" | sha512sum -c -; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$PROM_URL" -o "$PROM_JAR"; \
    echo "$PROM_SHA512  $PROM_JAR" | sha512sum -c -; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$S3_URL" -o "$S3_JAR"; \
    echo "$S3_SHA512  $S3_JAR" | sha512sum -c -

USER flink
