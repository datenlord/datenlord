#! /bin/sh

NODE1=`hostname -I | cut -d' ' -f1`

REGISTRY=quay.io/coreos/etcd
# available from v3.2.5
REGISTRY=gcr.io/etcd-development/etcd

type docker || { echo "Please install docker first!"; exit 1; }

NAME=etcd
#  --volume=${DATA_DIR}:/etcd-data \
ARCH=`uname -a | cut -d' ' -f13`
if [ $ARCH = 'aarch64' ]; then
        ETCD_VERSION=v3.4.0-arm64
        ETCD_ARCH_ENV='-e ETCD_UNSUPPORTED_ARCH=arm64'
elif [ $ARCH = 'x86_64' ]; then
        ETCD_VERSION=latest
        ETCD_ARCH_ENV=''
else
        echo "etcd can only run on x86_64 and arm64, unsupported architecture $ARCH"
        exit 1
fi

# delete exited etcd container if any
RES=`docker rm $NAME 2>/dev/null`
CMD="docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  -v /home/ubuntu/etcd/etcd-data:/etcd-data \
  -e ETCDCTL_API=3 $ETCD_ARCH_ENV \
  --name $NAME ${REGISTRY}:$ETCD_VERSION /usr/local/bin/etcd \
  --data-dir=/etcd-data --name node1 \
  --initial-advertise-peer-urls http://${NODE1}:2380 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --advertise-client-urls http://${NODE1}:2379 \
  --listen-client-urls http://0.0.0.0:2379 \
  --initial-cluster node1=http://${NODE1}:2380"
echo $CMD
sh -c "$CMD"
