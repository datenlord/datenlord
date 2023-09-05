#!/usr/bin/env bash

export CONTROLLER_SOCKET_FILE=/tmp/controller.sock
export BIND_MOUNTER=../target/debug/bind_mounter
export NODE_SOCKET_FILE=/tmp/node.sock
export RUST_BACKTRACE=full
export RUST_LOG=debug
export RUST_BACKTRACE=1
export ETCD_END_POINT=127.0.0.1:2379
export BIND_MOUNTER=`realpath $BIND_MOUNTER`

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/shell_env.sh
# setup etcd service
. $CURDIR/setup_etcd.sh

echo "==> Start to deploy datenlord locally"
echo "==> Building datenlord"
cargo build
if [ $? -ne 0 ]; then
  echo "Failed to build datenlord."
  exit 1
fi

# Unmount the directory if it's mounted
fusermount -u $DATENLORD_LOCAL_BIND_DIR

# Check if the directory exists
if [ -d $DATENLORD_LOCAL_BIND_DIR ]; then

  # Remove the directory
  rm -rf $DATENLORD_LOCAL_BIND_DIR
  if [ $? -ne 0 ]; then
    echo "Failed to remove directory."
    exit 1
  fi

  echo "Directory unmounted and removed."
fi

echo "Mounting... ... ... ..."

# Create mount point
mkdir $DATENLORD_LOCAL_BIND_DIR
if [ $? -ne 0 ]; then
  echo "Failed to create mount point."
  exit 1
fi


echo "Starting datenlord"
$CURDIR/../target/debug/datenlord start_node --endpoint=unix:///tmp/node.sock --workerport=0 --nodeid=localhost --nodeip=127.0.0.1 --drivername=io.datenlord.csi.plugin --mountpoint=$DATENLORD_LOCAL_BIND_DIR --etcd=127.0.0.1:2379 --volume_info="fuse-test-bucket;http://127.0.0.1:9000;test;test1234" --capacity=1073741824 --serverport=8800 --volume_type=none 