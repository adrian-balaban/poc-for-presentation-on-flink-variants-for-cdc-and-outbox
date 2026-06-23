# Kafka Connect image for the k8s stack.
#
# Differs from local-development-podman/kafka-connect/Dockerfile (Confluent base) because:
#   - Strimzi 1.0.x deploys Kafka 4.2.0; Confluent cp-kafka-connect:7.6.1 bundles
#     Kafka 3.7.0 client libs — the client/broker version gap is larger than ideal.
#   - Debezium 2.7.3 predates Kafka 4.x; Debezium 3.0.x adds Kafka 4.x support.
#   - The Strimzi base image is what the Strimzi operator expects for its managed
#     Kafka Connect lifecycle (entrypoint, plugin discovery path /opt/kafka/plugins/).
#
# Build context: repo root (so the COPY of the SMT jar resolves).
# Build tag:     kafka-connect-debezium:local (non-latest → IfNotPresent in k8s,
#                which works with kind's local containerd without a registry).
#
# Usage (via deploy.sh):
#   podman build -t kafka-connect-debezium:local \
#     -f local-development-k8s/kafka-connect/Dockerfile .
FROM quay.io/strimzi/kafka:1.0.1-kafka-4.2.0

USER root

# Debezium MySQL connector 3.5.2.Final — first branch with true Kafka 4.x support.
# 3.0.2.Final still calls KafkaConsumer.poll(long) which was removed in Kafka 4.0,
# causing NoSuchMethodError in KafkaSchemaHistory.recoverRecords(). 3.1+ removed
# that call; 3.5.2.Final is the latest stable release at the time of writing.
# Connector JARs go in /opt/kafka/plugins/debezium-mysql/; the Strimzi operator
# configures plugin.path=/opt/kafka/plugins so all subdirectories are scanned.
RUN mkdir -p /opt/kafka/plugins/debezium-mysql && \
    curl -fL \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.5.2.Final/debezium-connector-mysql-3.5.2.Final-plugin.tar.gz" \
    | tar -xz -C /opt/kafka/plugins/debezium-mysql/

# Custom SMT JAR (java-connect-smts, compiled Java 11 — runs fine on the Java 17
# JVM in this Strimzi image). Place in its own plugin dir for class-loader isolation.
# Run `./gradlew :kafka-connect-smts:shadowJar` before building this image.
RUN mkdir -p /opt/kafka/plugins/custom-smts
COPY local-development-podman/kafka-connect-smts/build/libs/*-with-deps.jar /opt/kafka/plugins/custom-smts/

USER 1001
