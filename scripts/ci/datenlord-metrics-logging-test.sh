#!/bin/sh

. scripts/setup/config.sh

# Need to wait for some time before metrics are collected by Prometheus
PROMETHEUS_WAIT_TIME=5 
NGINX_VERSION="1.21.5"

# Datanlord metrics test
docker pull nginx:${NGINX_VERSION}
kind load docker-image nginx:${NGINX_VERSION}
kubectl apply -f $DATENLORD_METRICS_TEST
kubectl wait --for=condition=Ready pod metrics-datenlord-test --timeout=120s
kubectl exec -i metrics-datenlord-test -- sh -c "echo test > /usr/share/nginx/html/testfile"
kubectl exec -i metrics-datenlord-test -- sh -c "cat /usr/share/nginx/html/testfile"
sleep $PROMETHEUS_WAIT_TIME
NODE_IP=`kubectl get nodes -A -o wide | awk 'FNR == 2 {print $6}'`

# Datanlord logging test
curl -s "http://${NODE_IP}:31001/k8s-csi-datenlord-*/_search" | python -c "import sys, json; assert json.load(sys.stdin)['hits']['total']['value'] > 0"
