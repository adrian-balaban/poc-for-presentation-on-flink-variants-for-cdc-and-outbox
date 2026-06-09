FROM flink:2.2

ARG FLINK_CDC_VERSION=3.6.0-2.2

USER root

RUN apt-get update && apt-get install -y --no-install-recommends gettext-base && rm -rf /var/lib/apt/lists/*

# Download the Flink-2.2-specific Flink CDC 3.6.0 distribution
RUN curl -fL \
      "https://archive.apache.org/dist/flink/flink-cdc-3.6.0/flink-cdc-${FLINK_CDC_VERSION}-bin.tar.gz" \
      | tar -xz -C /opt/ \
 && ln -s /opt/flink-cdc-${FLINK_CDC_VERSION} /opt/flink-cdc \
 && mkdir -p /opt/flink-cdc-${FLINK_CDC_VERSION}/log \
 && chmod 777 /opt/flink-cdc-${FLINK_CDC_VERSION}/log

# Add pipeline connector JARs (not bundled in the 3.6.0 dist), MySQL JDBC driver,
# and flink-shaded-guava31 which Flink 2.2 no longer ships as a separate JAR
RUN curl -fL \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/${FLINK_CDC_VERSION}/flink-cdc-pipeline-connector-mysql-${FLINK_CDC_VERSION}.jar" \
      -o /opt/flink-cdc/lib/flink-cdc-pipeline-connector-mysql-${FLINK_CDC_VERSION}.jar \
 && curl -fL \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/${FLINK_CDC_VERSION}/flink-cdc-pipeline-connector-kafka-${FLINK_CDC_VERSION}.jar" \
      -o /opt/flink-cdc/lib/flink-cdc-pipeline-connector-kafka-${FLINK_CDC_VERSION}.jar \
 && curl -fL \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
      -o /opt/flink-cdc/lib/mysql-connector-j-8.0.33.jar \
 && curl -fL \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-guava/31.1-jre-17.0/flink-shaded-guava-31.1-jre-17.0.jar" \
      -o /opt/flink/lib/flink-shaded-guava-31.1-jre-17.0.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER flink

ENV PATH=/opt/flink-cdc/bin:$PATH

# pipeline.yaml is bind-mounted at runtime; entrypoint expands env vars before submission
ENTRYPOINT ["/entrypoint.sh"]
