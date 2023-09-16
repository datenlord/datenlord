#!/bin/sh

. scripts/setup/config.sh

LOCAL_TEST=${1:-true}
if [ "$LOCAL_TEST" = true ]; then
  sed -i "s|WORKDIR /tmp/build|WORKDIR $(pwd)|" Dockerfile
  sed -i "s|COPY --from=builder /tmp/build/|COPY --from=builder $(pwd)/|" Dockerfile
fi
docker build . --build-arg RUST_IMAGE_VERSION=$RUST_VERSION --file ./Dockerfile --target datenlord --tag $DATENLORD_IMAGE

for image in $ETCD_IMAGE $BUSYBOX_IMAGE $SCHEDULER_IMAGE $CSI_ATTACHER_IMAGE $CSI_DRIVER_IMAGE $CSI_PROVISIONER_IMAGE $CSI_RESIZER_IMAGE $CSI_SNAPSHOTTER_IMAGE; do
  echo "loading $image"
  docker pull $image
  kind load docker-image $image
done
kind load docker-image $DATENLORD_IMAGE
kubectl cluster-info
kubectl get pods -A
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$SNAPSHOTTER_VERSION/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$SNAPSHOTTER_VERSION/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$SNAPSHOTTER_VERSION/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
kubectl apply -f $CONFIG_KIND
kubectl wait --for=condition=Ready pod -l app=$CONTROLLER_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
kubectl wait --for=condition=Ready pod -l app=$NODE_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
kubectl get csidriver
kubectl get csinode
kubectl get storageclass
kubectl get volumesnapshotclass
rm -f *rbac-snapshot-controller.yaml* *setup-snapshot-controller.yaml*
wget https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$SNAPSHOTTER_VERSION/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml
wget https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$SNAPSHOTTER_VERSION/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml
sed -e 's/namespace\:\ default/namespace\:\ kube\-system/g' rbac-snapshot-controller.yaml > datenlord-rbac-snapshot-controller.yaml
sed -e 's/namespace\:\ default/namespace\:\ kube\-system/g' setup-snapshot-controller.yaml > datenlord-setup-snapshot-controller.yaml
docker pull gcr.io/k8s-staging-sig-storage/snapshot-controller:${SNAPSHOTTER_VERSION}
kind load docker-image gcr.io/k8s-staging-sig-storage/snapshot-controller:${SNAPSHOTTER_VERSION}
kubectl apply -f datenlord-rbac-snapshot-controller.yaml
kubectl apply -f datenlord-setup-snapshot-controller.yaml
for i in $(seq 1 30); do
  if kubectl get deployment snapshot-controller -n kube-system; then
      break
  fi 
  sleep 1
done
kubectl wait --for=condition=Ready pod -l app=snapshot-controller -n kube-system --timeout=60s
kubectl get pods -A -o wide
# Sleep 60 to wait cluster become stable
sleep 60
kubectl get pods -A -o wide