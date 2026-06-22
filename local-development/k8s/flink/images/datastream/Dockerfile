# Artifact image for the DataStream variant — holds ONLY the fat-jar.
#
# The Flink Kubernetes Operator mounts this as a volume via an init-container:
# the FlinkDeployment's jobManager.podTemplate.spec.initContainers[0] uses
# `image: flink-cdc-artifact-datastream:latest` and copies the jar from
# /artifacts into an emptyDir (`flink-usrlib`) mounted at /opt/flink/usrlib on
# the JM pod. The job then runs it via:
#   jarURI:    local:///opt/flink/usrlib/datastream-job.jar
#   entryClass: poc.datastream.DataStreamCdcJob
#
# This is the official Flink Operator artifact pattern (see upstream
# examples/pod-template.yaml at release-1.15.0): the base Flink image
# (flink-with-mysql) stays generic and variant-agnostic — only this tiny
# artifact image differs per variant. busybox provides the `sh` + `cp` the
# init-container runs to populate the volume.
#
# Build (see k8s/deploy.sh): the fat-jar is copied to `job.jar` in this build
# context before `podman build`, so COPY picks up the freshly built artifact.
FROM busybox:1.35
COPY job.jar /artifacts/datastream-job.jar