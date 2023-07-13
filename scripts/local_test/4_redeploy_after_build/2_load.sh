source scripts/local_test/load_envs.sh


for image in $ETCD_IMAGE $BUSYBOX_IMAGE $SCHEDULER_IMAGE $CSI_ATTACHER_IMAGE $CSI_DRIVER_IMAGE $CSI_PROVISIONER_IMAGE $CSI_RESIZER_IMAGE $CSI_SNAPSHOTTER_IMAGE; do
  docker pull $image
  kind load docker-image $image
done
kind load docker-image $DATENLORD_IMAGE
kubectl cluster-info
kubectl get pods -A
kubectl apply -f scripts/local_test/4_redeploy_after_build/snapshot.storage.k8s.io_volumesnapshots.yaml
kubectl apply -f scripts/local_test/4_redeploy_after_build/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f scripts/local_test/4_redeploy_after_build/snapshot.storage.k8s.io_volumesnapshotclasses.yaml

echo "apply -f $CONFIG_KIND"
kubectl apply -f $CONFIG_KIND
kubectl wait --for=condition=Ready pod -l app=$CONTROLLER_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
kubectl wait --for=condition=Ready pod -l app=$NODE_APP_LABEL -n $DATENLORD_NAMESPACE --timeout=60s
FOUND_PATH=`cat /proc/self/mountinfo | grep fuse | grep $FUSE_MOUNT_PATH | awk '{print $5}'`
test -n $FOUND_PATH || (echo "FAILED TO FIND MOUNT PATH $FUSE_MOUNT_PATH" && /bin/false)
kubectl get pods -A -o wide
