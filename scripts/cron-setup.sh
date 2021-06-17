#!/bin/sh

#set -xv # enable debug
set -e # exit on error
set -u # unset var as error


DATENLORD_CSI_IMAGE="datenlord/csiplugin:e2e_test"
DATENLORD_IMAGE="datenlord/datenlord:e2e_test"
KIND_VERSION="v0.11.0"
KIND_NODE_VERSION="kindest/node:v1.21.1@sha256:fae9a58f17f18f06aeac9772ca8b5ac680ebbed985e266f711d936e91d113bad"
RUST_VERSION="1.47.0"

# Install Kind
echo "Install Kind"
curl -Lo ./kind https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin
# Create Kind Cluster
echo "Create Kind Cluster"
cat >> ./kind-config.yaml <<END
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
kind create cluster --config ./kind-config.yaml

# Setup Kind SSH
echo "Setup Kind SSH"
rm -rf $HOME/.ssh/
ssh-keygen -N '' -f ~/.ssh/id_rsa

NODES_IP="$(kubectl get nodes -A -o wide | awk 'FNR > 2 {print $6}')"
NODES="$(kubectl get nodes -A -o wide | awk 'FNR > 2 {print $1}')"
for node in ${NODES}; do
  USER="$(whoami)"
  docker exec ${node} apt-get update
  docker exec ${node} apt-get install -y ssh sudo
  docker exec ${node} systemctl start sshd
  docker exec ${node} useradd -m ${USER}
  docker exec ${node} usermod -aG sudo ${USER}
  echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /tmp/${USER}
  docker cp /tmp/${USER} ${node}:/etc/sudoers.d/${USER}
  docker exec ${node} chown root:root /etc/sudoers.d/${USER}

  docker exec ${node} mkdir /home/${USER}/.ssh
  docker cp ${HOME}/.ssh/id_rsa.pub ${node}:/home/${USER}/.ssh/authorized_keys
  docker exec ${node} chown ${USER}:${USER} /home/${USER}/ -R
done
for ip in ${NODES_IP}; do
    ssh-keyscan -H $ip >> ${HOME}/.ssh/known_hosts
done

# Docker build
echo "Docker build fuse and csi"
docker build . --build-arg RUST_IMAGE_VERSION=$RUST_VERSION --file ./Dockerfile --target fuse --tag $DATENLORD_IMAGE
docker build . --build-arg RUST_IMAGE_VERSION=$RUST_VERSION --file ./Dockerfile --target csi --tag $DATENLORD_CSI_IMAGE
kind load docker-image $DATENLORD_IMAGE
kind load docker-image $DATENLORD_CSI_IMAGE
