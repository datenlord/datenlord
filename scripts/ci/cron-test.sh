#!/bin/bash

source scripts/setup/config.sh

# Local Test?
LOCAL_TEST=${1:-true}

# Install kind
bash scripts/setup/install-kind.sh

# Create Kind Cluster
bash scripts/setup/create-kind-cluster.sh

# Setup SSH
bash scripts/setup/setup-ssh-for-cluster-nodes.sh

# Datenlord Monitoring and Alerting Test
bash scripts/ci/datenlord-monitor-test.sh

# Deploy DatenLord to K8S
bash scripts/setup/deploy-datenlord-to-k8s.sh $LOCAL_TEST

# Datenlord metric and logging test
bash scripts/ci/datenlord-metrics-logging-test.sh

# CSI E2E Test
bash scripts/ci/csi-e2e-test.sh

# DatenLord Perf Test
# TODO: avoid to re-create cluster
kind delete cluster
kind create cluster --config /tmp/kind-config.yaml
kind load docker-image ${DATENLORD_IMAGE}
bash scripts/perf/datenlord-perf-test.sh