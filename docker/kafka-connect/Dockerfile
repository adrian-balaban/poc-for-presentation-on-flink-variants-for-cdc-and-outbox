FROM confluentinc/cp-kafka-connect:7.6.1

# Install Debezium MySQL connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:latest

# Copy custom SMT JARs
COPY kafka-connect-smts/target/*.jar /usr/share/java/custom-smts/
ENV CLASSPATH="/usr/share/java/custom-smts/*:$CLASSPATH"

USER appuser
