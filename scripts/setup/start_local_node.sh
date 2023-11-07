#!/bin/sh

# scripts/setup/start_local_node.sh [build_flags]
#
# The parameter `build_flags` will be passed to `cargo build` in this scripts
# when building DatenLord. You should put all options in quotes.
#
# For example, run `scripts/setup/start_local_node.sh`, this script will just simply run `cargo build` without any option.
# But run `scripts/setup/start_local_node.sh "-F abi-7-23"`, this script will run `cargo build -F abi-7-23` to build DatenLord.

export CONTROLLER_SOCKET_FILE=/tmp/controller.sock
export BIND_MOUNTER=../target/debug/bind_mounter
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
./target/debug/datenlord start_node --endpoint=unix:///tmp/node.sock --workerport=0 --nodeid=localhost --nodeip=127.0.0.1 --drivername=io.datenlord.csi.plugin --mountpoint=$DATENLORD_LOCAL_BIND_DIR --etcd=127.0.0.1:2379 --volume_info="fuse-test-bucket;http://127.0.0.1:9000;test;test1234" --capacity=1073741824 --serverport=8800 --volume_type=none 
