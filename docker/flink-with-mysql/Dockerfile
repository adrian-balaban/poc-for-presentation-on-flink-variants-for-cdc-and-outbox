FROM docker.io/library/flink:2.2

USER root

RUN curl -fL \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
      -o /opt/flink/lib/mysql-connector-j-8.0.33.jar

USER flink
