FROM docker.io/library/flink:2.2

USER root

# Add the MySQL JDBC driver and the Prometheus metrics reporter — neither is
# pre-bundled in the flink:2.2 image, so both are downloaded from Maven.
# SHA-512 digests pinned to known-good artifacts (verified against Maven Central's
# published .sha1 at build-authoring time). Maven Central serves no .sha512 sidecar,
# and fetching a sidecar from the same host adds little protection — so we pin here.
# On a version bump, regenerate the digests with: sha512sum <downloaded-jar>
RUN set -eux; \
    MYSQL_URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"; \
    PROM_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/2.2.1/flink-metrics-prometheus-2.2.1.jar"; \
    MYSQL_JAR=/opt/flink/lib/mysql-connector-j-8.0.33.jar; \
    PROM_JAR=/opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-2.2.1.jar; \
    MYSQL_SHA512="dfc56aa76a7419a8ed2c0d051ed279ded8c9aff354018aac2f8c97b68a220784abe7a8ba4fc47e932c41bff61cb9637024a7e19cfaa37efd78cd8a3227f7baaa"; \
    PROM_SHA512="73cc78f88512afefb884a7705ad2aea23d8666984280f9350e2ecd3fa4af02e11c1c22a4afbec768717f2abff61844cb62b95e7c41a413f4f1b9abc6b72353fb"; \
    mkdir -p /opt/flink/plugins/metrics-prometheus; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$MYSQL_URL" -o "$MYSQL_JAR"; \
    echo "$MYSQL_SHA512  $MYSQL_JAR" | sha512sum -c -; \
    curl -fSL --retry 3 --retry-delay 2 --max-time 120 "$PROM_URL" -o "$PROM_JAR"; \
    echo "$PROM_SHA512  $PROM_JAR" | sha512sum -c -

USER flink
