# Artifact image for the Outbox variant (variant-4) — holds ONLY the fat-jar.
#
# Same init-container + volume pattern as the DataStream variant (see
# images/datastream/Dockerfile): the FlinkDeployment's
# jobManager.podTemplate.spec.initContainers[0] uses
# `image: flink-cdc-artifact-outbox:latest` and copies the jar from
# /artifacts into the emptyDir (`flink-usrlib`) mounted at /opt/flink/usrlib on
# the JM pod. The job then runs it via:
#   jarURI:    local:///opt/flink/usrlib/outbox-job.jar
#   entryClass: poc.outbox.OutboxJob
#
# Build (see k8s/deploy.sh): the fat-jar is copied to `job.jar` in this build
# context before `podman build`, so COPY picks up the freshly built artifact.
FROM busybox:1.35
COPY job.jar /artifacts/outbox-job.jar