#!/bin/bash

if [ "$1" == "helm" ]
then
    kubectl create namespace datenlord-monitoring 
    helm repo add stable https://charts.helm.sh/stable
    helm install prometheus stable/prometheus-operator --namespace datenlord-monitoring 
    helm repo add elastic https://helm.elastic.co
    helm install filebeat elastic/filebeat
    helm install kibana elastic/kibana
    helm install elasticsearch elastic/elasticsearch -f values.yaml
else
    kubectl apply -f datenlord-logging.yaml
    kubectl apply -f datenlord-monitor.yaml
fi
