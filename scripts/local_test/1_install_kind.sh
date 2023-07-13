source scripts/local_test/load_envs.sh


curl -Lo ./kind https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin