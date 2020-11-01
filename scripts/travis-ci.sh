#!/bin/bash

function show_result()
{
    rv="$1"
    tag="$2"

    if [ "$rv" -eq 0 ]; then
        echo "$tag: passed"
    else
        echo "$tag: failed with $rv"
    fi

    return $rv
}

function run_tests()
{
    # NOTE: run non-root test of async_fuse
    set -o pipefail
    cargo test -p async_fuse 2>&1 | tee cargo_test.log
    rv=$?
    set +o pipefail

    show_result $rv "non-root test"
    if [ "$rv" -ne 0 ]; then
        return $rv
    fi

    # NOTE: run root test of async_fuse
    test_bin_path=`grep Running cargo_test.log | awk '{print $2}'`
    echo $test_bin_path
    sudo $test_bin_path
    rv=$?

    show_result $rv "root test"
    if [ "$rv" -ne 0 ]; then
        return $rv
    fi

    # TODO: run test of csi
    return 0
}

function teardown()
{
    rm cargo_test.log
}


run_tests
rv=$?
teardown
exit $rv
