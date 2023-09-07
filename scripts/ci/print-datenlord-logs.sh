#!/bin/bash

source scripts/setup/config.sh

echo “check disk usage”
df -h

kubectl get pods -A -o wide
CONTROLLER_POD_NAME=`kubectl get pod -l app=$CONTROLLER_APP_LABEL -n $DATENLORD_NAMESPACE -o jsonpath="{.items[0].metadata.name}"`
echo "SHOW LOGS OF $CONTROLLER_CONTAINER_NAME IN $CONTROLLER_POD_NAME"
kubectl logs $CONTROLLER_POD_NAME -n $DATENLORD_NAMESPACE -c $CONTROLLER_CONTAINER_NAME
NODE_POD_NAMES=`kubectl get pods --selector=app=${NODE_APP_LABEL} --namespace ${DATENLORD_NAMESPACE} --output=custom-columns="NAME:.metadata.name" | tail -n +2`
for pod in ${NODE_POD_NAMES}; do
echo "SHOW LOGS OF $NODE_CONTAINER_NAME IN ${pod}"
kubectl logs ${pod} -n ${DATENLORD_NAMESPACE} -c ${NODE_CONTAINER_NAME}
done
kubectl describe pod metrics-datenlord-test
kubectl cluster-info dump