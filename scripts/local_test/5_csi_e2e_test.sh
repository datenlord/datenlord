source scripts/local_test/load_envs.sh


wget --quiet https://dl.k8s.io/$K8S_VERSION/kubernetes-test-linux-amd64.tar.gz
tar zxvf kubernetes-test-linux-amd64.tar.gz
kubectl config view --raw > $K8S_CONFIG
#kubernetes/test/bin/e2e.test -v=5 -ginkgo.failFast -ginkgo.failOnPending -ginkgo.debug -ginkgo.v -ginkgo.focus='External.Storage' -kubectl-path=`which kubectl` -kubeconfig=$K8S_CONFIG -storage.testdriver=`realpath $E2E_TEST_CONFIG`
kubernetes/test/bin/ginkgo -v -failFast -failOnPending -debug -focus='External.Storage' -skip='\[Feature:|\[Disruptive\]|\[Serial\]' kubernetes/test/bin/e2e.test -- -v=5 -kubectl-path=`which kubectl` -kubeconfig=`realpath $K8S_CONFIG` -storage.testdriver=`realpath $E2E_TEST_CONFIG` | tee csi_e2e_test1.log 
# Run [Disruptive] test in serial and separately
kubernetes/test/bin/ginkgo -v -failFast -failOnPending -debug -focus='External.Storage.*(\[Feature:|\[Disruptive\]|\[Serial\])' kubernetes/test/bin/e2e.test -- -v=5 -kubectl-path=`which kubectl` -kubeconfig=`realpath $K8S_CONFIG` -storage.testdriver=`realpath $E2E_TEST_CONFIG` | tee csi_e2e_test2.log 