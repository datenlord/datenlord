#!/bin/sh

. scripts/setup/config.sh

# Previous version is v1.8.0, but in docker ci environment, it will fail to pull the image
# [DEPRECATION NOTICE] Docker Image Format v1 and Docker Image manifest version 2, schema 1 support is disabled by default
# and will be removed in an upcoming release. Suggest the author of quay.io/coreos/kube-state-metrics:v1.8.0
# to upgrade the image to the OCI Format or Docker Image manifest v2, schema 2.
# More information at https://docs.docker.com/go/deprecated-image-specs/
KUBE_STATE_METRICS_VERSION=${KUBE_STATE_METRICS_VERSION:-"v1.9.8"}
ALERTMANAGER_VERSION=${ALERTMANAGER_VERSION:-"v0.19.0"}
PROMETHEUS_VERSION=${PROMETHEUS_VERSION:-"v2.30.3"}
GRAFANA_VERSION=${GRAFANA_VERSION:-"8.2.0"}
FLUENTD_VERSION=${FLUENTD_VERSION:-"v3.0.1"}
ELASTIICSEARCH_VERSION=${ELASTIICSEARCH_VERSION:-"7.4.2"}
KIBANA_VERSION=${KIBANA_VERSION:-"7.2.0"}

# Datenlord Monitoring test
docker pull quay.io/coreos/kube-state-metrics:$KUBE_STATE_METRICS_VERSION
docker pull prom/alertmanager:$ALERTMANAGER_VERSION
docker pull prom/prometheus:$PROMETHEUS_VERSION
docker pull grafana/grafana:$GRAFANA_VERSION
docker pull quay.io/fluentd_elasticsearch/fluentd:$FLUENTD_VERSION
docker pull docker.elastic.co/elasticsearch/elasticsearch-oss:$ELASTIICSEARCH_VERSION
docker pull docker.elastic.co/kibana/kibana-oss:$KIBANA_VERSION
docker pull prom/node-exporter
kind load docker-image quay.io/coreos/kube-state-metrics:$KUBE_STATE_METRICS_VERSION
kind load docker-image prom/prometheus:$PROMETHEUS_VERSION
kind load docker-image grafana/grafana:$GRAFANA_VERSION
kind load docker-image quay.io/fluentd_elasticsearch/fluentd:$FLUENTD_VERSION
kind load docker-image prom/alertmanager:$ALERTMANAGER_VERSION
kind load docker-image docker.elastic.co/elasticsearch/elasticsearch-oss:$ELASTIICSEARCH_VERSION
kind load docker-image docker.elastic.co/kibana/kibana-oss:$KIBANA_VERSION
kind load docker-image prom/node-exporter

cp scripts/setup/alertmanager-alerts.yaml scripts/setup/alertmanager-alerts-backup.yaml
cat scripts/ci/alertmanager-test-alert.yaml >> scripts/setup/alertmanager-alerts.yaml
sh scripts/setup/datenlord-monitor-deploy.sh deploy
# restore the alertmanager-alerts.yaml
mv -f scripts/setup/alertmanager-alerts-backup.yaml scripts/setup/alertmanager-alerts.yaml

NODE_IP=`kubectl get nodes -A -o wide | awk 'FNR == 2 {print $6}'`
FOUND_PATH=`curl --silent $NODE_IP:30000 | grep Found`
test -n "$FOUND_PATH" || (echo "FAILED TO FIND PROMETHEUS SERVICE" && /bin/false)

# Datenlord Alerting test
n=0
until [ "$n" -ge 5 ]
do
  echo "checking"
  if curl --silent $NODE_IP:31000/api/v2/alerts | grep 'High Memory Usage';
  then
    exit
  else
    echo "waiting 15s"
    n=$((n+1))
    sleep 15
  fi
done

echo "FAILED TO FIND PROMETHEUS SERVICE" && /bin/false
