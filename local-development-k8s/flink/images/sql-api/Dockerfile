# Artifact image for the SQL API variant (variant-3) — holds ONLY the fat-jar.
#
# Same init-container + volume pattern as the DataStream variant (see
# images/datastream/Dockerfile): the FlinkDeployment's
# jobManager.podTemplate.spec.initContainers[0] uses
# `image: flink-cdc-artifact-sql-api:latest` and copies the jar from
# /artifacts into the emptyDir (`flink-usrlib`) mounted at /opt/flink/usrlib on
# the JM pod. The job then runs it via:
#   jarURI:    local:///opt/flink/usrlib/sql-api-job.jar
#   entryClass: poc.sqlapi.SqlApiCdcJob
#
# Build (see k8s/deploy.sh): the fat-jar is copied to `job.jar` in this build
# context before `podman build`, so COPY picks up the freshly built artifact.
FROM busybox:1.35
COPY job.jar /artifacts/sql-api-job.jar