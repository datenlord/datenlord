export CONTROLLER_SOCKET_FILE=/tmp/controller.sock
export BIND_MOUNTER=target/debug/bind_mounter
export ETCD_CONTAINER_NAME=etcd
export ETCD_IMAGE=gcr.io/etcd-development/etcd:v3.4.13
export NODE_SOCKET_FILE=/tmp/node.sock
export RUST_BACKTRACE=full
export RUST_LOG=debug
export RUST_BACKTRACE=1

cargo build

sudo chown root:root $BIND_MOUNTER
sudo chmod u+s $BIND_MOUNTER
ls -lsh $BIND_MOUNTER
export ETCD_END_POINT=127.0.0.1:2379
export BIND_MOUNTER=`realpath $BIND_MOUNTER`

export ETCD_END_POINT=127.0.0.1:2379
sudo sed -i 's/#user_allow_other/user_allow_other/g' /etc/fuse.conf

fusermount -u /tmp/datenlord_data_dir
rm -rf /tmp/datenlord_data_dir
mkdir /tmp/datenlord_data_dir

echo "Starting datenlord"
target/debug/datenlord start_node --endpoint=unix:///tmp/node.sock --workerport=0 --nodeid=localhost --nodeip=127.0.0.1 --drivername=io.datenlord.csi.plugin --mountpoint=/tmp/datenlord_data_dir --etcd=127.0.0.1:2379 --volume_info="fuse-test-bucket;http://127.0.0.1:9000;test;test1234" --capacity=1073741824 --serverport=8800 --volume_type=none 
# target/debug/datenlord start_csi_controller --endpoint=unix:///tmp/controller.sock --workerport=0 --nodeid=localhost --nodeip=127.0.0.1 --drivername=io.datenlord.csi.plugin --mountpoint=/tmp/datenlord_data_dir --etcd=127.0.0.1:2379