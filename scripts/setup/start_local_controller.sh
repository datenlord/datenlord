#!/bin/sh

export CONTROLLER_SOCKET_FILE=/tmp/controller.sock
export BIND_MOUNTER=./target/debug/bind_mounter
export NODE_SOCKET_FILE=/tmp/node.sock
export RUST_BACKTRACE=full
export RUST_LOG=debug
export RUST_BACKTRACE=1
export ETCD_END_POINT=127.0.0.1:2379
export BIND_MOUNTER=`realpath $BIND_MOUNTER`

. scripts/setup/config.sh

# Check if the node socket file exists, we should run node before running controller
# If the node socket file does not exist, we should run node first
if [ ! -S $NODE_SOCKET_FILE ]; then
  echo "Node is not running. Start node first and try again."
  exit 1
fi

# The node socket file exists, but the node may not be running
# Use nc to check if the node is running
if ! nc -z -U $NODE_SOCKET_FILE; then
    echo "Node socket file exists, but the node may not be running."
    echo "Remove the node socket file......"
    rm -f $NODE_SOCKET_FILE
    echo "Start node first and try again."
    exit 1
fi

# Make sure the controller is not running
if [ -S $CONTROLLER_SOCKET_FILE ]; then
  # Use nc to check if the controller is running
    if nc -z -U $CONTROLLER_SOCKET_FILE; then
        echo "Controller is already running. Stop the controller first and try again."
        exit 1
    fi
    # Socket file exists, but the controller may not be running
fi

# Build the controller and bind_mounter
echo "==> Building controller and bind_mounter"
cargo build --bin datenlord --bin bind_mounter
if [ $? -ne 0 ]; then
  echo "Failed to build controller."
  exit 1
fi

# Set bind_mounter's owner to root and set the setuid bit
sudo chown root:root $BIND_MOUNTER
sudo chmod u+s $BIND_MOUNTER
ls -lsh $BIND_MOUNTER

# Start the controller
echo "==> Starting controller"
target/debug/datenlord --role=controller --csi-endpoint=unix://$CONTROLLER_SOCKET_FILE --csi-worker-port=0 --node-name=localhost --node-ip=127.0.0.1 --csi-driver-name=io.datenlord.csi.plugin --mount-path=$DATENLORD_LOCAL_BIND_DIR --kv-server-list=$ETCD_END_POINT