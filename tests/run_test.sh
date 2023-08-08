#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

echo "==> Run fstest"
if [ -e "./fstest/fstest" ]; then
  cd $DATENLORD_LOCAL_BIND_DIR
  prove -rv $CURDIR/fstest/tests
  exit $?
else
  cd ./fstest
  make
fi
echo "==> All test passed"