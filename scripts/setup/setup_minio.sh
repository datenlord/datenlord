#!/bin/sh

# Source the configuration
. scripts/setup/config.sh

echo "==> Pulling MinIO image"
# docker pull $MINIO_IMAGE

# Check if the MinIO container is already running and remove it if it is
if [ "$(docker ps -a -q -f name=$MINIO_CONTAINER_NAME 2> /dev/null)" ]; then
    echo "==> Stopping and removing container $MINIO_CONTAINER_NAME"
    docker rm -f $MINIO_CONTAINER_NAME
fi

# Start the new MinIO container
echo "==> Starting container $MINIO_CONTAINER_NAME"
docker run -d --rm \
  --name $MINIO_CONTAINER_NAME \
  -p $CONSOLE_PORT:$CONSOLE_PORT \
  -p $MINIO_PORT:$MINIO_PORT \
  -e "MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY" \
  -e "MINIO_SECRET_KEY=$MINIO_SECRET_KEY" \
  $MINIO_IMAGE server /data \
  --console-address ":$CONSOLE_PORT"

# Create the bucket
echo "Waiting for MinIO to start..."
sleep 5 # wait for 5 seconds to allow MinIO server to start
docker exec $MINIO_CONTAINER_NAME \
  /bin/sh -c "mkdir -p /data/$MINIO_BUCKET"

echo "==> MinIO setup complete"
docker ps
docker logs $MINIO_CONTAINER_NAME
