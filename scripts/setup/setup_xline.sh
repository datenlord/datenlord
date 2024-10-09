#!/bin/sh

. scripts/setup/config.sh

echo "==> Pulling image $XLINE_IMAGE"
docker pull $XLINE_IMAGE

# Check if the container is already running and remove it if it is
if [ "$(docker ps -a -q -f name=$XLINE_CONTAINER_NAME 2> /dev/null)" ]; then
    echo "==> Stopping and removing container $XLINE_CONTAINER_NAME" 
  docker rm -f $XLINE_CONTAINER_NAME
fi

# Start the new container
echo "==> Starting container $XLINE_CONTAINER_NAME"

docker run -it --rm -d --name=$XLINE_CONTAINER_NAME -e RUST_LOG=xline=debug -v /usr/local/xline/data-dir:/usr/local/xline/data-dir -p 2379:2379 $XLINE_IMAGE \
    $XLINE_CONTAINER_NAME \
    --name $XLINE_CONTAINER_NAME \
    --storage-engine rocksdb \
    --members $XLINE_CONTAINER_NAME=$XLINE_END_POINT \
    --data-dir /usr/local/xline/data-dir \
    --client-listen-urls http://0.0.0.0:2379 \
    --peer-listen-urls http://0.0.0.0:2380 \
    --client-advertise-urls $XLINE_END_POINT \
    --peer-advertise-urls http://127.0.0.1:2380

docker ps
docker logs $XLINE_CONTAINER_NAME
