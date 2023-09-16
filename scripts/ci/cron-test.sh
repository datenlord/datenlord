#!/bin/sh

. scripts/setup/config.sh

# Local Test?
LOCAL_TEST=${1:-true}

# Install kind
sh scripts/setup/install-kind.sh

# Create Kind Cluster
sh scripts/setup/create-kind-cluster.sh

# Setup SSH
sh scripts/setup/setup-ssh-for-cluster-nodes.sh

# Datenlord Monitoring and Alerting Test
sh scripts/ci/datenlord-monitor-test.sh

# Deploy DatenLord to K8S
sh scripts/setup/deploy-datenlord-to-k8s.sh $LOCAL_TEST

# Datenlord metric and logging test
sh scripts/ci/datenlord-metrics-logging-test.sh

# CSI E2E Test
sh scripts/ci/csi-e2e-test.sh

# DatenLord Perf Test
# TODO: avoid to re-create cluster
kind delete cluster
kind create cluster --config /tmp/kind-config.yaml
kind load docker-image ${DATENLORD_IMAGE}
sh scripts/perf/datenlord-perf-test.sh