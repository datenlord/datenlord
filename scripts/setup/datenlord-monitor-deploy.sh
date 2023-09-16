#!/bin/sh

. scripts/setup/config.sh

# wait for port forward to be ready
PORT_FORWARD_WAIT_TIME=3

print_usage ()
{
    cat <<EOF
Usage: $0 [deploy|undeploy|helm]

deploy      Deploy datenlord monitor by yaml file.
helm        Deploy datenlord monitor by helm.
undeploy    Undeploy all datenlord monitor.
EOF
}

if [ $# -le 0 ]
then
    print_usage
    exit 1
fi

if [ "$1" = "helm" ]
then
    kubectl create namespace datenlord-monitoring
    kubectl create namespace datenlord-logging 
    helm repo add stable https://charts.helm.sh/stable
    helm install prometheus stable/prometheus-operator --namespace datenlord-monitoring 
    helm repo add elastic https://helm.elastic.co
    helm install filebeat elastic/filebeat --namespace datenlord-logging 
    helm install kibana elastic/kibana --namespace datenlord-logging 
    helm install elasticsearch elastic/elasticsearch -f elasticsearch-values.yaml --namespace datenlord-logging 
    kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=grafana -n datenlord-monitoring --timeout=60s
    kubectl wait --for=condition=Ready pod -l app=elasticsearch-master -n datenlord-logging --timeout=120s
    kubectl wait --for=condition=Ready pod -l app=kibana -n datenlord-logging --timeout=120s
    kubectl wait --for=condition=Ready pod -l app=prometheus -n datenlord-logging --timeout=120s
    POD_NAME=`kubectl get pods -l app.kubernetes.io/name=grafana | grep grafana | awk '{print $1}'`
    kubectl port-forward $POD_NAME $GRAFANA_PORT -n datenlord-monitoring &
elif [ "$1" = "undeploy" ]
then
    kubectl delete all --all -n datenlord-monitoring
    kubectl delete all --all -n datenlord-logging
elif [ "$1" = "deploy" ]
then
    sed -e "s/ALERTMANAGER_ADDRESS/'alertmanager.datenlord-monitoring.svc:8080'/g" \
        -e "s/KUBE_STATE_METRICS_ADDRESS/['kube-state-metrics.datenlord-monitoring.svc.cluster.local:8080']/g" \
        -e "s/NODE_EXPORTER_NAME/'node_exporter'/g" scripts/setup/datenlord-monitor.yaml > scripts/datenlord-monitor-deploy.yaml
    kubectl apply -f scripts/setup/alertmanager-alerts.yaml
    kubectl apply -f scripts/setup/datenlord-logging.yaml
    kubectl apply -f scripts/datenlord-monitor-deploy.yaml
    kubectl wait --for=condition=Ready pod -l app=prometheus-server -n datenlord-monitoring --timeout=60s
    kubectl wait --for=condition=Ready pod -l app=grafana -n datenlord-monitoring --timeout=60s
    kubectl wait --for=condition=Ready pod -l app=kibana -n datenlord-logging --timeout=120s
    kubectl wait --for=condition=Ready pod -l app=elasticsearch -n datenlord-logging --timeout=120s
    POD_NAME=`kubectl get pods -l app=grafana -n datenlord-monitoring | grep grafana | awk '{print $1}'`
    kubectl port-forward $POD_NAME $GRAFANA_PORT -n datenlord-monitoring &
    sleep $PORT_FORWARD_WAIT_TIME
else
    print_usage
    exit 1
fi