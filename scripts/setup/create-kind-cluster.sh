#!/bin/sh

. scripts/setup/config.sh

rm -f /tmp/kind-config.yaml
cat >> /tmp/kind-config.yaml << END
# Kind cluster with 1 control plane node and 3 workers
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
# the control plane node config
- role: control-plane
# the three workers
- role: worker
  image: $KIND_NODE_VERSION
- role: worker
  image: $KIND_NODE_VERSION
- role: worker
  image: $KIND_NODE_VERSION
END

kind create cluster --config /tmp/kind-config.yaml