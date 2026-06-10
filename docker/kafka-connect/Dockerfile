FROM docker.io/confluentinc/cp-kafka-connect:7.6.1

# Install Debezium MySQL connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:latest

# Copy custom SMT JARs (built with gradle shadowJar)
COPY kafka-connect-smts/build/libs/kafka-connect-smts-*-with-deps.jar /usr/share/java/custom-smts/

# Add a startup script wrapper that ensures plugin path is set
RUN echo '#!/bin/bash' > /tmp/entrypoint.sh && \
    echo 'export CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"' >> /tmp/entrypoint.sh && \
    echo 'exec /etc/confluent/docker/run "$@"' >> /tmp/entrypoint.sh && \
    chmod +x /tmp/entrypoint.sh

ENTRYPOINT ["/tmp/entrypoint.sh"]

USER appuser
