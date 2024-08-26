#!/bin/sh

. scripts/setup/config.sh

# Local Test?
LOCAL_TEST=${1:-true}

# Install kind
echo "Install Kind"
sh scripts/setup/install-kind.sh || exit 1

# Create Kind Cluster
echo "Create Kind Cluster"
sh scripts/setup/create-kind-cluster.sh || exit 1

# Setup SSH
echo "Setup SSH"
sh scripts/setup/setup-ssh-for-cluster-nodes.sh || exit 1

# Datenlord Monitoring and Alerting Test
# echo "Datenlord Monitoring and Alerting Test"
# sh scripts/ci/datenlord-monitor-test.sh || exit 1

# Deploy DatenLord to K8S
echo "Deploy DatenLord to K8S"
sh scripts/setup/deploy-datenlord-to-k8s.sh $LOCAL_TEST || exit 1

# Datenlord metric and logging test
echo "Datenlord metric and logging test"
# sh scripts/ci/datenlord-metrics-logging-test.sh || exit 1

# CSI E2E Test
echo "CSI E2E Test"
sh scripts/ci/csi-e2e-test.sh || exit 1

# DatenLord Perf Test
# TODO: avoid to re-create cluster
echo "DatenLord Perf Test"
kind delete cluster
kind create cluster --config /tmp/kind-config.yaml
kind load docker-image ${DATENLORD_IMAGE}
sh scripts/perf/datenlord-perf-test.sh || exit 1