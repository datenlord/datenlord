#!/bin/sh

# scripts/setup/start_local_node.sh [build_flags]
#
# The parameter `build_flags` will be passed to `cargo build` in this scripts
# when building DatenLord. You should put all options in quotes.
#
# For example, run `scripts/setup/start_local_node.sh`, this script will just simply run `cargo build` without any option.
# But run `scripts/setup/start_local_node.sh "-F abi-7-23"`, this script will run `cargo build -F abi-7-23` to build DatenLord.

export CONTROLLER_SOCKET_FILE=/tmp/controller.sock
export BIND_MOUNTER=./target/debug/bind_mounter
export NODE_SOCKET_FILE=/tmp/node.sock
export RUST_BACKTRACE=full
export RUST_LOG=debug
export RUST_BACKTRACE=1
export ETCD_END_POINT=127.0.0.1:2379
export BIND_MOUNTER=`realpath $BIND_MOUNTER`

# build flags with `cargo build`
BUILD_FLAGS=$1

. scripts/setup/config.sh
. scripts/setup/setup_etcd.sh

if mount | grep -q "$DATENLORD_LOCAL_BIND_DIR"; then
    echo "$DATENLORD_LOCAL_BIND_DIR is mounted. Unmounting now."
    fusermount -u $DATENLORD_LOCAL_BIND_DIR
else
    echo "$DATENLORD_LOCAL_BIND_DIR is not mounted."
fi

# Check if the directory exists
if [ -d $DATENLORD_LOCAL_BIND_DIR ]; then
  # Remove the directoryS
  rm -rf $DATENLORD_LOCAL_BIND_DIR
  if [ $? -ne 0 ]; then
    echo "Failed to remove directory $DATENLORD_LOCAL_BIND_DIR."
    exit 1
  fi
  echo "$DATENLORD_LOCAL_BIND_DIR unmounted and removed."
fi

# Set bind_mounter's owner to root and set the setuid bit
sudo chown root:root $BIND_MOUNTER
sudo chmod u+s $BIND_MOUNTER
ls -lsh $BIND_MOUNTER

echo "==> Start to deploy datenlord locally"
echo "==> Building datenlord"
cargo build $BUILD_FLAGS
if [ $? -ne 0 ]; then
  echo "Failed to build datenlord."
  exit 1
fi


echo "Mounting... ... ... ..."

# Create mount point(/tmp/datenlord_data_dir)
mkdir $DATENLORD_LOCAL_BIND_DIR
if [ $? -ne 0 ]; then
  echo "Failed to create mount point $DATENLORD_LOCAL_BIND_DIR"
  exit 1
fi

echo "Starting datenlord.. ... ... ..."
cargo run $BUILD_FLAGS --bin=datenlord -- \
--role=node \
--csi-endpoint=unix:///tmp/node.sock \
--csi-worker-port=0 \
--node-name=localhost \
--node-ip=127.0.0.1 \
--csi-driver-name=io.datenlord.csi.plugin \
--mount-path=$DATENLORD_LOCAL_BIND_DIR \
--kv-server-list=127.0.0.1:2379 \
--storage-fs-root=/tmp/datenlord_backend \
--server-port=8800 \
--storage-type=fs \
--storage-mem-cache-write-back