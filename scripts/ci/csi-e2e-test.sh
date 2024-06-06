#!/bin/sh

. scripts/setup/config.sh

# CSI E2E Test
# clean up nodes temporarily
cat > /tmp/clean_up_mount_dir.sh <<'END'
#!/bin/sh

CONFIG_KIND=$1
NODE_APP_LABEL=$2
DATENLORD_NAMESPACE=$3
IF_REDEPLOY=$4

kubectl delete -f $CONFIG_KIND
NODES_IP="$(kubectl get nodes -A -o wide | awk 'FNR > 2 {print $6}')"
USER="$(whoami)"
for ip in ${NODES_IP}; do
  ssh -t ${USER}@${ip} 'sudo sh -s' < scripts/setup/umount-in-container.sh /var/opt/datenlord-data
  echo "done umount in node $ip"
done

if [ "$IF_REDEPLOY" = true ]; then
  kubectl apply -f $CONFIG_KIND
  kubectl wait --for=condition=Ready pod -l app=$NODE_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=240s
  sleep 120
  kubectl get pods -A -o wide
fi
END


if [ ! -d "kubernetes" ]; then
  wget --quiet https://dl.k8s.io/$K8S_VERSION/kubernetes-test-linux-amd64.tar.gz
  tar zxvf kubernetes-test-linux-amd64.tar.gz
fi

kubectl config view --raw > $K8S_CONFIG
echo "Ginkgo test"
# kubernetes/test/bin/ginkgo -v -failOnPending -debug -focus='External.Storage' -skip='\[Feature:|\[Disruptive\]|\[Serial\]' kubernetes/test/bin/e2e.test -- -v=5 -kubectl-path=`which kubectl` -kubeconfig=`realpath $K8S_CONFIG` -storage.testdriver=`realpath $E2E_TEST_CONFIG`
kubernetes/test/bin/ginkgo -v -failOnPending -debug -focus='should be able to unmount after the subpath directory is deleted' -skip='\[Feature:|\[Disruptive\]|\[Serial\]' kubernetes/test/bin/e2e.test -- -v=5 -kubectl-path=`which kubectl` -kubeconfig=`realpath $K8S_CONFIG` -storage.testdriver=`realpath $E2E_TEST_CONFIG`
/bin/sh /tmp/clean_up_mount_dir.sh $CONFIG_KIND $NODE_APP_LABEL $DATENLORD_NAMESPACE true
# Run [Disruptive] test in serial and separately
# kubernetes/test/bin/ginkgo -v -failFast -failOnPending -debug -focus='External.Storage.*(\[Feature:|\[Disruptive\]|\[Serial\])' kubernetes/test/bin/e2e.test -- -v=5 -kubectl-path=`which kubectl` -kubeconfig=`realpath $K8S_CONFIG` -storage.testdriver=`realpath $E2E_TEST_CONFIG`
# /bin/sh /tmp/clean_up_mount_dir.sh $CONFIG_KIND $NODE_APP_LABEL $DATENLORD_NAMESPACE false
