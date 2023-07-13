source scripts/local_test/load_envs.sh


rm -f ./kind-config.yaml

cat >> ./kind-config.yaml <<END
# Kind cluster with 1 control plane node and 3 workers
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
# the control plane node config
- role: control-plane
  image: $KIND_NODE_VERSION
# the three workers
- role: worker
  image: $KIND_NODE_VERSION
- role: worker
  image: $KIND_NODE_VERSION
- role: worker
  image: $KIND_NODE_VERSION
END
kind create cluster --config ./kind-config.yaml
