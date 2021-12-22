#/bin/sh

set -xv # enable debug
set -e # exit on error
set -u # unset var as error

readonly PERF_CONFIG="scripts/datenlord-perf.yaml"
readonly NAMESPACE="csi-datenlord"
readonly ASYNC_FUSE_APP="datenlord-async-fuse"
readonly FIO_SCRIPT="fio_perf.fio"
readonly TEST_DIR="/var/opt/datenlord-data"
readonly TEST_LOG="/tmp/perf_log.txt"

kubectl apply -f ${PERF_CONFIG}
kubectl wait --for=condition=Ready pod -l app=${ASYNC_FUSE_APP} -n ${NAMESPACE} --timeout=60s
# Sleep 10s to make sure the cluster is stable
sleep 10


FIRST_NODE=$(kubectl get pods -A | grep "datenlord-async" | awk 'NR==1{print $2}')

kubectl exec ${FIRST_NODE} -n ${NAMESPACE} -- apt-get update
kubectl exec ${FIRST_NODE} -n ${NAMESPACE} -- apt-get install -y fio
kubectl cp scripts/${FIO_SCRIPT} ${FIRST_NODE}:/tmp/${FIO_SCRIPT} -n ${NAMESPACE}
kubectl exec ${FIRST_NODE} -n ${NAMESPACE} -- env TEST_DIR=${TEST_DIR} fio /tmp/${FIO_SCRIPT} > ${TEST_LOG}
