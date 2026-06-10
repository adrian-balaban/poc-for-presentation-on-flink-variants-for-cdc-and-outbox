FROM docker.io/confluentinc/cp-kafka-connect:7.6.1

# Install Debezium MySQL connector via direct download (avoid Confluent Hub versioning issues)
# Latest compatible with Java 11 is 2.x; confluent-hub may have version mismatches
RUN curl -sL -o /tmp/debezium.jar https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.7.3.Final/debezium-connector-mysql-2.7.3.Final-plugin.tar.gz && \
    tar -xzf /tmp/debezium.jar -C /usr/share/java/ && \
    rm /tmp/debezium.jar

# Copy the shadow (with-deps) SMT JAR only — the plain jar is compiled for Java 17
# while cp-kafka-connect:7.6.1 runs Java 11, causing UnsupportedClassVersionError.
COPY docker/kafka-connect-smts/build/libs/*-with-deps.jar /usr/share/java/custom-smts/

ENTRYPOINT ["/etc/confluent/docker/run"]

USER appuser
