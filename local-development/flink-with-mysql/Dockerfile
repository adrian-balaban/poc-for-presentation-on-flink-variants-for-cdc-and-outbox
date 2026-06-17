FROM docker.io/library/flink:2.2

USER root

# Increase JVM metaspace to prevent OOM during job submissions
ENV FLINK_JVM_OPTIONS="-XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m"

# Add the MySQL JDBC driver, Prometheus metrics reporter, S3 filesystem plugin (hadoop), and RocksDB state backend.
# All are downloaded from Maven Central with pinned SHA-512 digests.
# On a version bump, regenerate the digests with: sha512sum <downloaded-jar>
# Note: using flink-s3-fs-hadoop instead of presto for better MinIO compatibility
RUN set -eux; \
    MYSQL_URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"; \
    PROM_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/2.2.1/flink-metrics-prometheus-2.2.1.jar"; \
    S3_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/2.2.1/flink-s3-fs-hadoop-2.2.1.jar"; \
    ROCKSDB_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-statebackend-rocksdb/2.2.1/flink-statebackend-rocksdb-2.2.1.jar"; \
    MYSQL_JAR=/opt/flink/lib/mysql-connector-j-8.0.33.jar; \
    PROM_JAR=/opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-2.2.1.jar; \
    S3_JAR=/opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-2.2.1.jar; \
    ROCKSDB_JAR=/opt/flink/plugins/rocksdb/flink-statebackend-rocksdb-2.2.1.jar; \
    MYSQL_SHA512="dfc56aa76a7419a8ed2c0d051ed279ded8c9aff354018aac2f8c97b68a220784abe7a8ba4fc47e932c41bff61cb9637024a7e19cfaa37efd78cd8a3227f7baaa"; \
    PROM_SHA512="73cc78f88512afefb884a7705ad2aea23d8666984280f9350e2ecd3fa4af02e11c1c22a4afbec768717f2abff61844cb62b95e7c41a413f4f1b9abc6b72353fb"; \
    S3_SHA512="d9f8bb9f2f74dfeb1f18ace6714fa208265b2486624e55980bb9044f52c23167464507dc7ddd9c6ea8e5c7a0ab101d3d6ff2b1c70964537ffcf0a6573605fa80"; \
    ROCKSDB_SHA512="d9e8deeba75ee5a5e640d047dbb108f1ebb6c5151ed312c2a84d0395c706539ba282016021d00db3542ebd096c23cbf46d3dd66e843f1906412fe7bbad9b2507"; \
    mkdir -p /opt/flink/plugins/metrics-prometheus /opt/flink/plugins/s3-fs-hadoop /opt/flink/plugins/rocksdb; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$MYSQL_URL" -o "$MYSQL_JAR"; \
    echo "$MYSQL_SHA512  $MYSQL_JAR" | sha512sum -c -; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$PROM_URL" -o "$PROM_JAR"; \
    echo "$PROM_SHA512  $PROM_JAR" | sha512sum -c -; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$S3_URL" -o "$S3_JAR"; \
    echo "$S3_SHA512  $S3_JAR" | sha512sum -c -; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$ROCKSDB_URL" -o "$ROCKSDB_JAR"; \
    echo "$ROCKSDB_SHA512  $ROCKSDB_JAR" | sha512sum -c -

USER flink
