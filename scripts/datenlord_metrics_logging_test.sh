#! /bin/sh

set -o errexit
set -o nounset
set -o xtrace

# Need to wait for some time before metrics are collected by Promethues
PROMETHUES_WAIT_TIME=5 

# Datanlord metrics test
kubectl apply -f $DATENLORD_METRICS_TEST
kubectl wait --for=condition=Ready pod metrics-datenlord-test --timeout=60s
kubectl exec -i metrics-datenlord-test -- bash -c "echo test > /usr/share/nginx/html/testfile"
kubectl exec -i metrics-datenlord-test -- bash -c "cat /usr/share/nginx/html/testfile"
sleep $PROMETHUES_WAIT_TIME
NODE_IP=`kubectl get nodes -A -o wide | awk 'FNR == 2 {print $6}'`

# Datanlord logging test
curl -s "http://${NODE_IP}:31001/k8s-csi-datenlord-*/_search" | python -c "import sys, json; assert json.load(sys.stdin)['hits']['total']['value'] > 0"
