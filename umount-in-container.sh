#! /bin/sh

set -xv # enable debug
set -e # exit on error
set -u # unset var as error

if [ $# -le 0 ]
then
    echo "Plase input mount directory\nthe usage: $0 <MOUNT DIR>"
    exit 1
fi

FUSECTL_DIR=/sys/fs/fuse/connections

FUSE_DIR=$1
MOUNTED=$(cat /proc/self/mountinfo | grep fuse | grep $FUSE_DIR | awk '{print $5}')
if [ -z $MOUNTED ]
then
    echo "$FUSE_DIR UN-MOUNTED"
else
    umount -f $FUSE_DIR || echo "UMOUNT FAILED"
fi

FUSE_MINOR=$(cat /proc/self/mountinfo | grep fuse | grep $FUSE_DIR | awk '{print $3}' | cut -d ':' -f 2)
if [ -z $FUSE_MINOR ]
then
    echo "$FUSE_DIR IS NOT MOUNTED"
else
    FUSE_CTL_MOUNTED=$(cat /proc/self/mountinfo | grep $FUSECTL_DIR | awk '{print $5}')
    if [ $FUSE_CTL_MOUNTED ]
    then
        echo "FUSECTL IS MOUNTED"
    else
        echo "MOUNT FUSECTL"
        mount -t fusectl fusectl $FUSECTL_DIR
    fi
    echo "UMOUNT FUSE DIR=$FUSE_DIR MINOR=$FUSE_MINOR" | tee /sys/fs/fuse/connections/$FUSE_MINOR/abort
fi
