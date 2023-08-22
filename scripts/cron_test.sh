#!/bin/bash

source scripts/config.sh

# Local Test?
LOCAL_TEST=${1:-true}

# Install kind
bash scripts/install_kind.sh

# Create Kind Cluster
bash scripts/create_kind_cluster.sh

# Setup SSH
bash scripts/setup_ssh_for_cluster_nodes.sh

# Datenlord Monitoring and Alerting Test
bash scripts/datenlord_monitor_test.sh

# Deploy DatenLord to K8S
bash scripts/deploy_datenlord_to_k8s.sh $LOCAL_TEST

# Datenlord metric and logging test
bash scripts/datenlord_metrics_logging_test.sh

# CSI E2E Test
bash scripts/csi_e2e_test.sh

# DatenLord Perf Test
# TODO: avoid to re-create cluster
kind delete cluster
kind create cluster --config /tmp/kind-config.yaml
kind load docker-image ${DATENLORD_IMAGE}
bash scripts/datenlord_perf_test.sh