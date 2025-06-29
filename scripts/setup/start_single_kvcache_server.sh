#!/bin/sh

# scripts/setup/start_single_kvcache_server.sh [build_flags]
#
# The parameter `build_flags` will be passed to `cargo build` in this scripts
# when building DatenLord. You should put all options in quotes.
#

. scripts/setup/config.sh
. scripts/setup/setup_etcd.sh

echo "==> Start to deploy datenlord locally"
echo "==> Building datenlord"
cargo build $BUILD_FLAGS
if [ $? -ne 0 ]; then
  echo "Failed to build datenlord."
  exit 1
fi

# Start kvcache server with basic configuration
echo "Starting datenlord kvcache server......"
cargo run $BUILD_FLAGS --bin=kvcache