# DatenLord

----
*DatenLord* is a next-generation cloud-native distributed storage platform, which aims to meet the performance-critical storage needs from next-generation cloud-native applications, such as microservice, serverless, AI, etc. On one hand, DatenLord is designed to be a cloud-native storage system, which itself is distributed, fault-tolerant, and graceful upgrade. These cloud-native features make DatenLord easy to use and easy to maintain. On the other hand, DatenLord is designed as an application-orientated storage system, in that DatenLord is optimized for many performance-critical scenarios, such as databases, AI machine learning, big data. Meanwhile, DatenLord provides high-performance storage service for containers, which facilitates stateful applications running on top of Kubernetes (K8S). The high performance of DatenLord is achieved by leveraging the most recent technology revolution in hardware and software, such as NVMe, non-volatile memory, asynchronous programming, and the native Linux asynchronous IO support. 

---
## Why DatenLord?

Why do we build DatenLord? The reason is two-fold:
* Firstly, the recent computer hardware architecture revolution stimulates storage software refractory.  The storage related functionalities inside Linux kernel haven't changed much in recent 10 years, whenas hard-disk drive (HDD) was the main storage device. Nowadays, solid-state drive (SSD) becomes the mainstream, not event mention the most advanced SSD, NVMe and non-volatile memory. The performance of SSD is hundreds of times faster than HDD, in that the HDD latency is around 1~10 ms, whereas the SSD latency is around 50–150 μs, the NVMe latency is around 25 μs, and the non-volatile memory latency is 350 ns. With the performance revolution of storage devices, traditional blocking-style/synchronous IO in Linux kernel becomes very inefficient, and non-blocking-style/asynchronous IO is much more applicable. The Linux kernel community already realized that, and recently Linux kernel has proposed native-asynchronous IO mechanism, io_uring, to improve IO performance. Beside blocking-style/synchronous IO, the context switch overhead in Linux kernel becomes no longer negligible w.r.t. SSD latency. Many modern programming languages have proposed asynchronous programming, green thread or coroutine to manage asynchronous IO tasks in user space, in order to avoid context switch overhead introduced by blocking IO. Therefore we think it’s time to build a next-generation storage system that takes advantage of the storage performance revolution as far as possible, by leveraging non-blocking/asynchronous IO, asynchronous programming, NVMe, and even non-volatile memory, etc.

* Secondly, most distributed/cloud-native systems are computing and storage isolated, that computing tasks/applications and storage systems are of dedicated clusters, respectively. This isolated architecture is best to reduce maintenance, that it decouples the maintenance tasks of computing clusters and storage clusters into separate ones, such as upgrade, expansion, migration of each cluster respectively, which is much simpler than of coupled clusters. Nowadays, however, applications are dealing with much larger dataset than ever before. One notorious example is that an AI training job takes one hour to load data whereas the training job itself finishes in only 45 minutes. Therefore, isolating computing and storage makes IO very inefficient, as transferring data between applications and storage systems via network takes quite a lot time. Further, with the isolated architecture, applications have to aware of the different data location, and the varying access cost due to the difference of data location, network distance, etc. DatenLord tackles the IO performance issue of isolated architecture in a novel way, which abstracts the heterogeneous storage details and make the difference of data location, access cost, etc, transparent to applications. Furthermore, with DatenLord, applications can assume all the data to be accessed are local, and DatenLord will access the data on behalf of applications. Besides, DatenLord can help K8S to schedule jobs close to cached data, since DatenLord knows the exact location of all cached data. By doing so, applications are greatly simplified w.r.t. to data access, and DatenLord can leverage local cache, neighbor cache, and remote cache to speed up data access, so as to boost performance.

----
## Target scenarios

The main scenario of DatenLord is to facilitate high availability across multi-cloud, hybrid-cloud, multiple data centers, etc. Concretely, there are many online business providers whose business is too important to afford any downtime. To achieve high availability, the service providers have to leverage multi-cloud, hybrid-cloud, and multiple data centers to hopefully avoid single point failure of each single cloud or data center, by deploying applications and services across multiple clouds or data centers. It's relatively easier to deploy applications and services to multiple clouds and data centers, but it's much harder to duplicate all data to all clouds or all data centers in a timely manner, due to the huge data size. If data are not equally available across multiple clouds or data centers, the online business might still suffer from single point failure of a cloud or a data center, because data unavailability resulted from a cloud or a data center failure. 

DatenLord can alleviate data unavailable of cloud or data center failure by caching data to multiple layers, such as local cache, neighbor cache, remote cache, etc. Although total data size is huge, the hot data involved in online business is usually of limited size, which is call data locality. DatenLord leverages data locality and builds a set of large scale distributed and automatic cache layers to buffer hot data in a smart manner. The benefit of DatenLord is two-fold:
* DatenLord is transparent to applications, namely DatenLord does not need any modification to applications;
* DatenLord is high performance, that it automatically caches data by means of the data hotness, and it's performance is achieved by applying different caching strategies according to target applications. For example, least recent use (LRU) caching strategy for some kind of random access, most recent use (MRU) caching strategy for some kind of sequential access, etc.

----

## Architecture

![DatenLord Architecture](docs/images/Computing%20Defined%20Storage%402x.png "DatenLord Overall Architecture")

DatenLord is of master-slave architecture. To achieve better storage performance, DatenLord has a coupled architecture with K8S, that DatenLord can be deployed within a K8S cluster, in order to leverage data locality to speed up data access. The above figure is the overall DatenLord architecture, the green parts are DatenLord components, the blue parts are K8S components, the yellow part represents containerized applications. There are several major components of DatenLord: master node (marked as DatenLord), slave node (marked as Daten Sklavin), and K8S plugins.

The master node has three parts: S3 compatible interface (S3I), Lord, and Meta Storage Engine (MSE). S3I provides a convenient way to read and write data in DatenLord via S3 protocol, especially for bulk upload and download scenarios, e.g. uploading large amounts of data for big data batch jobs or AI machine learning training jobs. Lord is the overall controller of DatenLord, which controls all the internal behaviors of DatenLord, such as where and how to write data, synchronize data, etc. MSE stores all the meta information of DatenLord, such as the file paths of all the data stored in each slave node, the user-defined labels of each data file, etc. MSE is similar to HDFS namenode.

The slave node has four parts: Data Storage Engine (DSE), Sklavin, Meta Storage Engine (MSE), S3/P2P interface. DSE is the distributed cache layer, which is in charge of local IO and network IO, that it not only reads/writes data from/to memory or local disks, but also queries neighbor nodes to read neighbor cached data, further if local and neighbor cache missed, it reads data from remote persistent storage, and it can write data back to remote storage if necessary. More specifically, DatenLord sets up a filesystem in userspace (FUSE) in a slave node. DSE implements the FUSE API's, executing all the underlying FUSE operations, such as open, create, read, and write, etc. DSE also functions as a distributed cache, every DSE of slave nodes can communicate with each other via TCP/IP or RDMA to exchange cached data. Sklavin is to communicate with the Lord of the master node and handle the requests from the Lord and CSI driver, such as health check report, data synchronization, data consistency inspection, Lord election, etc. The MSE of the slave node is a local copy of the MSE from the master node. S3 interface provides a convenient way to read, write and synchronize data in a slave node.

The K8S plugins include a container storage interface (CSI) driver and a customer filter. The CSI driver is for DatenLord to work with K8S to manage volumes for container tasks, such as loading a read-only volume, creating a read-write volume. The customer filter is to help K8S to schedule tasks to data nearby based on the meta-information in MSE of the master node.
<!---
## DatenLord Optimization Strategy

In general, there are two kinds of storage needs from an application perspective: one is latency-sensitive, and the other is throughput-sensitive. As for latency-sensitive applications, such as database applications, like MySQL, MongoDB, and ElasticSearch, etc, their performance relies on how fast a single I/O-request got handled. As for throughput-sensitive applications, such as big data and AI machine learning jobs, like Spark, Hadoop, and TensorFlow, etc, the more data load per unit time, the better their performance.

DatenLord is crafted to fit the aforementioned two scenarios. Specifically, to reduce latency, DatenLord caches in memory as much data as possible, in order to minimize disk access; to improve throughput (we focus on reading throughput currently), DatenLord, on one hand, prefetches data in memory to speed up access, on the other hand, leverages K8S to schedule tasks to data nearby, so as to minimize data transfer cost.

## Target Usage Scenarios

DatenLord has several target scenarios, which fall into two main categories:
* Latency-sensitive cases, that DatenLord will coordinate with K8S to schedule containers close to data to minimize latency:
    * Containerized applications, especially stateful applications;
    * Serverless, Lambda, FaaS, etc, event-driven tasks;
* Throughput-sensitive cases, that DatenLord will pre-load remote data into local clusters to speed up access:
    * AI and big-data jobs, especially training tasks;
    * Multi-cloud storage unified management, to facilitate application migration across clouds.
-->
## Quick Start
Currently DatenLord has been built as Docker images and can be deployed via K8S.

To deploy DatenLord via K8S, just simply run:
* `sed -e 's/e2e_test/latest/g' scripts/datenlord.yaml > datenlord-deploy.yaml`
* `kubectl apply -f datenlord-deploy.yaml`

To use DatenLord, just define PVC using DatenLord Storage Class, and then deploy a Pod using this PVC:
```
cat <<EOF >datenlord-demo.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: pvc-datenlord-test
spec:
  accessModes:
    - ReadWriteOnce
  volumeMode: Filesystem
  resources:
    requests:
      storage: 100Mi
  storageClassName: csi-datenlord-sc

---
apiVersion: v1
kind: Pod
metadata:
  name: mysql-datenlord-test
spec:
    containers:
    - name: mysql
      image: mysql
      env:
      - name: MYSQL_ROOT_PASSWORD
        value: "rootpasswd"
      volumeMounts:
      - mountPath: /var/lib/mysql
        name: data
        subPath: mysql
    volumes:
    - name: data
      persistentVolumeClaim:
        claimName: pvc-datenlord-test
EOF

kubectl apply -f datenlord-demo.yaml
```

It may need to install snapshot CRD and controller on K8S, if used K8S CSI snapshot feature:
* `kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml`
* `kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml`
* `kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml`
* `kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml`
* `kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml`


## How to Contribute

Anyone interested in DatenLord is welcomed to contribute.

### Coding Style

Please follow the [code style](docs/coding_style.md). Meanwhile, DatenLord adopts very strict clippy linting, please fix every clippy warning before submit your PR. Also please make sure all CI tests are passed.

### Continuous Integration (CI)

The CI of DatenLord leverages GitHut Action. There are two CI flows for DatenLord, [One](.github/workflows/ci.yml) is for Rust cargo test, clippy lints, and standard filesystem E2E checks; [The other](.github/workflows/cron.yml) is for CSI related tests, such as CSI sanity test and CSI E2E test.

The CSI E2E test setup is a bit complex, its action script [cron.yml](.github/workflows/cron.yml) is quite long, so let's explain it in detail:
* First, it sets up a test K8S cluster with one master node and three slave nodes, using Kubernetes in Docker (KinD);

* Second, CSI E2E test requires no-password SSH login to each K8S slave node, since it might run some commands to prepare test environment or verify test result, so it has to setup SSH key to each Docker container of KinD slave nodes;

* Third, it builds DatenLord container images and loads to KinD, which is a caveat of KinD, in that KinD puts K8S nodes inside Docker containers, thus kubelet cannot reach any resource of local host, and KinD provides load operation to make the container images from local host visible to kubelet;

* At last, it deploys DatenLord to the test K8S cluster, then downloads pre-build K8S E2E binary, runs in parallel by involking `ginkgo -p`, and only selects `External.Storage` related CSI E2E testcases to run.

### Sub-Projects

DatenLord has several related sub-projects, mostly working in progress, listed alphabetically:
* [async-fuse](../async-fuse) Native async Rust library for FUSE;
* [async-rdma](../async-rdma) Async and safe Rust library for RDMA;
* [etcd-client](../etcd-client) Async etcd client SDK in Rust;
* [lockfree-cuckoohash](../lockfree-cuckoohash) Lock-free hashmap in Rust;
* [LordOS](../LordOS) Pure containerized Linux distribution;
* [ring-io](../ring-io) Async and safe Rust library for io_uring;
* [s3-server](../s3-server) S3 server in Rust.

## Road Map
- [ ] 0.1 Refactor async fuse lib to provide clear async APIs, which is used by the datenlord filesystem.
- [ ] 0.2 Support all Fuse APIs in the datenlord fs.
- [ ] 0.3 Make fuse lib fully asynchronous. Switch async fuse lib's device communication channel from blocking I/O to `io_uring`.
- [ ] 0.4 Complete K8S integration test.
- [ ] 0.5 Support RDMA.
- [ ] 1.0 Complete Tensorflow K8S integration and finish performance comparison with raw fs.
