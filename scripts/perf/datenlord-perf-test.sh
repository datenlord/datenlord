#/bin/sh

set -xv # enable debug
set -e # exit on error
set -u # unset var as error

readonly PERF_CONFIG="scripts/perf/datenlord-perf.yaml"
readonly NAMESPACE="csi-datenlord"
readonly ASYNC_FUSE_APP="datenlord-async-fuse"
readonly TEST_DIR="/var/opt/datenlord-data"
readonly OUTPUT_DIR="/tmp/output"
readonly PERF_SCRIPT="fio-perf-test.sh"
readonly FIO_VERIFY_CONFIG="scripts/perf/write_and_verify.fio"

# Deploy datenlord for perf test
kubectl apply -f ${PERF_CONFIG}
# Sleep 60s to make sure the cluster is stable
sleep 60
kubectl wait --for=condition=Ready pod -l app=${ASYNC_FUSE_APP} -n ${NAMESPACE} --timeout=120s

FIRST_NODE=$(kubectl get pods -A | grep "datenlord-async" | awk 'NR==1{print $2}')

kubectl exec ${FIRST_NODE} -n ${NAMESPACE} -- apt-get update
kubectl exec ${FIRST_NODE} -n ${NAMESPACE} -- apt-get install -y fio python3-pip
kubectl exec ${FIRST_NODE} -n ${NAMESPACE} -- pip3 install matplotlib numpy fio-plot==1.0.28
kubectl cp scripts/perf/${PERF_SCRIPT} ${FIRST_NODE}:/tmp -n ${NAMESPACE}
kubectl cp ${FIO_VERIFY_CONFIG} ${FIRST_NODE}:/tmp -n ${NAMESPACE}
kubectl exec ${FIRST_NODE} -n ${NAMESPACE} -- sh /tmp/${PERF_SCRIPT} ${TEST_DIR}

rm -rf ${OUTPUT_DIR}
mkdir ${OUTPUT_DIR}
kubectl cp ${FIRST_NODE}:${OUTPUT_DIR} -n ${NAMESPACE} ${OUTPUT_DIR}
