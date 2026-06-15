FROM docker.io/library/flink:2.2

USER root

# Add the MySQL JDBC driver and the Prometheus metrics reporter — neither is
# pre-bundled in the flink:2.2 image, so both are downloaded from Maven.
RUN curl -fL \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
      -o /opt/flink/lib/mysql-connector-j-8.0.33.jar && \
    mkdir -p /opt/flink/plugins/metrics-prometheus && \
    curl -fL \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/2.2.1/flink-metrics-prometheus-2.2.1.jar" \
      -o /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-2.2.1.jar

USER flink
