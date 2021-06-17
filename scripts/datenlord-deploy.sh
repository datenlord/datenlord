#!/bin/sh

#set -xv # enable debug
set -e # exit on error
set -u # unset var as error

SCRIPT_DIR="$(dirname $(realpath $0))"
DATENLORD_ETCD="${SCRIPT_DIR}/datenlord-etcd.yaml"
DATENLORD="${SCRIPT_DIR}/datenlord.yaml"

CONTROLLER_APP_LABEL="csi-controller-datenlord"
DATENLORD_NAMESPACE="csi-datenlord"
ETCD_APP_LABEL="csi-etcd"
NODE_APP_LABEL="csi-nodeplugin-datenlord"

usage() {
    echo "Usage: $0 [deploy|undeploy]"
}

if [ $# -ne 1 ]; then
    usage
    exit 1
fi

if [ $1 = "deploy" ]; then
    kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
    kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
    kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
    kubectl apply -f ${DATENLORD_ETCD}
    kubectl wait --for=condition=Ready pod -l app=$ETCD_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
    kubectl apply -f ${DATENLORD}
    kubectl wait --for=condition=Ready pod -l app=$CONTROLLER_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
    kubectl wait --for=condition=Ready pod -l app=$NODE_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
elif [ $1 = "undeploy" ]; then
    kubectl delete -f ${DATENLORD}
    kubectl delete -f ${DATENLORD_ETCD}
else
    usage
    exit 1
fi
