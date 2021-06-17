#!/bin/sh

#set -xv # enable debug
set -e # exit on error
set -u # unset var as error

E2E_TEST_CONFIG="scripts/datenlord-e2e-test.yaml"
FUSE_MOUNT_PATH="/var/opt/datenlord-data"
K8S_CONFIG="k8s.e2e.config"
K8S_VERSION="v1.21.1"

usage() {
    echo "Usage: $0 [deploy|run]"
}

if [ $# -ne 1 ]; then
    usage
    exit 1
fi

if [ $1 = "deploy" ]; then
    # Deploy DatenLord to K8S
    echo "Deploy DatenLord to K8S"
    #kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
    #kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
    #kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
    #kubectl apply -f $DATENLORD_ETCD
    #kubectl wait --for=condition=Ready pod -l app=$ETCD_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
    #kubectl apply -f $CONFIG_KIND
    #kubectl wait --for=condition=Ready pod -l app=$CONTROLLER_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
    #kubectl wait --for=condition=Ready pod -l app=$NODE_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s

    ./scripts/datenlord-deploy.sh deploy
    FOUND_PATH=`cat /proc/self/mountinfo | grep fuse | grep $FUSE_MOUNT_PATH | awk '{print $5}'`
    test -n $FOUND_PATH || (echo "FAILED TO FIND MOUNT PATH $FUSE_MOUNT_PATH" && /bin/false)
    #kubectl delete -f $CONFIG_KIND
    #kubectl delete -f $DATENLORD_ETCD
    ./scripts/datenlord-deploy.sh undeploy
    NO_PATH=`cat /proc/self/mountinfo | grep fuse | grep $FUSE_MOUNT_PATH | awk '{print $5}'`
    test -z $NO_PATH || (echo "FAILED TO UN-MOUNT PATH $FUSE_MOUNT_PATH" && /bin/false)
    #kubectl apply -f $DATENLORD_ETCD
    #kubectl wait --for=condition=Ready pod -l app=$ETCD_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
    #kubectl apply -f $CONFIG_KIND
    ./scripts/datenlord-deploy.sh deploy
    kubectl get csidriver
    kubectl get csinode
    kubectl get storageclass
    kubectl get volumesnapshotclass
    #kubectl wait --for=condition=Ready pod -l app=$CONTROLLER_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
    #kubectl wait --for=condition=Ready pod -l app=$NODE_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
    wget https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml
    wget https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml
    sed -e 's/namespace\:\ default/namespace\:\ kube\-system/g' rbac-snapshot-controller.yaml > datenlord-rbac-snapshot-controller.yaml
    sed -e 's/namespace\:\ default/namespace\:\ kube\-system/g' setup-snapshot-controller.yaml > datenlord-setup-snapshot-controller.yaml
    kubectl apply -f datenlord-rbac-snapshot-controller.yaml
    kubectl apply -f datenlord-setup-snapshot-controller.yaml
    kubectl wait --for=condition=Ready pod -l app=snapshot-controller -n kube-system --timeout=60s
    kubectl get pods -A -o wide

elif [ $1 = "run" ]; then
    # Run CSI E2E Test
    echo "Run CSI E2E Test"
    wget --quiet https://dl.k8s.io/$K8S_VERSION/kubernetes-test-linux-amd64.tar.gz
    tar zxvf kubernetes-test-linux-amd64.tar.gz
    kubectl config view --raw > $K8S_CONFIG
    kubernetes/test/bin/ginkgo -p -v -failFast -failOnPending -debug -focus='External.Storage' -skip='.*Dynamic PV.*filesystem.*should access to two volumes with the same volume mode and retain data across pod recreation on .* node' kubernetes/test/bin/e2e.test -- -v=5 -kubectl-path=`which kubectl` -kubeconfig=`realpath $K8S_CONFIG` -storage.testdriver=`realpath $E2E_TEST_CONFIG` -test.parallel=3
else
    usage
    exit 1
fi
