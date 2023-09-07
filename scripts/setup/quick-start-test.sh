#!/bin/bash

source scripts/setup/config.sh

sed -e 's/e2e_test/latest/g' $CONFIG_KIND > $CONFIG_DOCKERHUB
kubectl apply -f $CONFIG_DOCKERHUB
kubectl wait --for=condition=Ready pod -l app=$CONTROLLER_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=120s
kubectl wait --for=condition=Ready pod -l app=$NODE_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=120s