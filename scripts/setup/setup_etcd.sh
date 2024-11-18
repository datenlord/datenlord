#!/bin/sh

. scripts/setup/config.sh

echo "==> Pulling image $ETCD_IMAGE"
docker pull $ETCD_IMAGE

# Check if the container is already running and remove it if it is
if [ "$(docker ps -a -q -f name=$ETCD_CONTAINER_NAME 2> /dev/null)" ]; then
    echo "==> Stopping and removing container $ETCD_CONTAINER_NAME"
  docker rm -f $ETCD_CONTAINER_NAME
fi

# Start the new container
echo "==> Starting container $ETCD_CONTAINER_NAME"
docker run -d --rm --net host --name $ETCD_CONTAINER_NAME $ETCD_IMAGE
docker ps
docker logs $ETCD_CONTAINER_NAME

