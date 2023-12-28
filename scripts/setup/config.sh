#!/bin/sh

export RUST_VERSION=1.74.0
export BUSYBOX_IMAGE=busybox:1.35.0
export CONFIG_DOCKERHUB=datenlord-deploy.yaml
export CONFIG_KIND=scripts/setup/datenlord.yaml
export CONTROLLER_APP_LABEL=csi-controller-datenlord
export CONTROLLER_CONTAINER_NAME=datenlord-controller-plugin
export CSI_ATTACHER_IMAGE=quay.io/k8scsi/csi-attacher:v2.2.0
export CSI_DRIVER_IMAGE=quay.io/k8scsi/csi-node-driver-registrar:v1.3.0
export CSI_PROVISIONER_IMAGE=quay.io/k8scsi/csi-provisioner:v1.6.0
export CSI_RESIZER_IMAGE=quay.io/k8scsi/csi-resizer:v0.5.0
export CSI_SNAPSHOTTER_IMAGE=quay.io/k8scsi/csi-snapshotter:v2.1.1
export DATENLORD_CSI_IMAGE=datenlord/csiplugin:e2e_test
export DATENLORD_IMAGE=ghcr.io/datenlord/datenlord:e2e_test
export DATENLORD_LOGGING=scripts/setup/datenlord-logging.yaml
export DATENLORD_LOGGING_NAMESPACE=datenlord-logging
export DATENLORD_METRICS_TEST=scripts/ci/datenlord-metrics-test.yaml
export DATENLORD_MONITORING=scripts/setup/datenlord-monitor.yaml
export DATENLORD_MONITORING_NAMESPACE=datenlord-monitoring
export DATENLORD_NAMESPACE=csi-datenlord
export E2E_TEST_CONFIG=scripts/ci/datenlord-e2e-test.yaml
export ELASTICSEARCH_LABEL=elasticsearch
export ETCD_IMAGE=gcr.io/etcd-development/etcd:v3.4.13
export FUSE_CONTAINER_NAME=datenlord-async
export FUSE_MOUNT_PATH=/var/opt/datenlord-data
export GRAFANA_LABEL=grafana
export GRAFANA_PORT=3000
export K8S_CONFIG=k8s.e2e.config
export K8S_VERSION=v1.21.1
export KIBANA_LABEL=kibana
export KIND_NODE_VERSION=kindest/node:v1.21.1@sha256:69860bda5563ac81e3c0057d654b5253219618a22ec3a346306239bba8cfa1a6
export KIND_VERSION=0.11.1
export NODE_APP_LABEL=csi-nodeplugin-datenlord
export NODE_CONTAINER_NAME=datenlord
export PROMETHEUS_LABEL=prometheus-server
export SCHEDULER_IMAGE=k8s.gcr.io/kube-scheduler:v1.19.1
export SNAPSHOTTER_VERSION=v5.0.0
export ETCD_CONTAINER_NAME=etcd
export DATENLORD_LOCAL_BIND_DIR=/tmp/datenlord_data_dir

set -xv # enable debug
set -e # exit on error
set -u # unset var as error