FROM docker.io/confluentinc/cp-kafka-connect:7.6.1

# Install Debezium MySQL connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:latest

# Copy custom SMT JARs (built with gradle shadowJar)
COPY kafka-connect-smts/build/libs/kafka-connect-smts-*-with-deps.jar /usr/share/java/custom-smts/
ENV CLASSPATH="/usr/share/java/custom-smts/*:$CLASSPATH"

USER appuser
