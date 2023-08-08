#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/shell_env.sh

# Check if the image already exists
if [ ! "$(docker images -q $ETCD_IMAGE 2> /dev/null)" ]; then
    echo "==> Pulling image $ETCD_IMAGE"
  docker pull $ETCD_IMAGE
else
    echo "==> Image $ETCD_IMAGE already exists."
fi


# Check if the container is already running and remove it if it is
if [ "$(docker ps -a -q -f name=$ETCD_CONTAINER_NAME 2> /dev/null)" ]; then
    echo "==> Stopping and removing container $ETCD_CONTAINER_NAME" 
  docker rm -f $ETCD_CONTAINER_NAME
fi

# Start the new container
echo "==> Starting container $ETCD_CONTAINER_NAME"
docker run -d --rm --net host --name $ETCD_CONTAINER_NAME $ETCD_IMAGE 
