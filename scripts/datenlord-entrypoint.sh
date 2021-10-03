#!/bin/sh

#set -xv # enable debug
#set -e # exit on error
#set -u # unset var as error

ARGS=$@
while [ $# -gt 0 ]; do
    key=$1
    case $key in
        -m | --mountpoint)
            MOUNTPOINT=$2
            break
            ;;
        -m=* | --mountpoint=*)
            MOUNTPOINT=${key#*=}
            break
            ;;
        *)
            shift
            ;;
    esac
done

/usr/local/bin/umount-in-container.sh $MOUNTPOINT
/usr/local/bin/datenlord-fuse $ARGS
