# DatenLord

![Logo of DatenLord](docs/images/logo.png)

----
*DatenLord* is an application-orientated, cloud-native distributed storage system. On one hand, DatenLord is designed as an application-orientated storage system, which is called "Computing Defined Storage", in that DatenLord is optimized for many application senarios, such as database, big data, AI machine leanring, etc. On the other hand, DatenLord is desinged as a cloud-native storage system, which works smoothly with cloud-native applications, especially container ecosystem. DatenLord provides high-performance storage service for containers, which facilitates stateful applications running on top of Kubernetes (K8S).

In general, there are two kinds of storage needs from application perspective: one is *latency-sensitive*, and the other is *throughput-sensitive*.
As for latency-sensitive applications, such as database applications, like MySQL, MongoDB, and ElasticSearch, etc, their performance relies on how fast a single I/O-request got handled.
As for throughput-sensitive applications, such as big-data and AI machine learning jobs, like Spark, Hadoop, and TensorFlow, etc, the more data load per unit time, the better performance.

DatenLord is crafted to fit the aforementioned two senarios.
Specifically, to reduce latency, DatenLord caches in memory as much data as possible, in order to minimize disk access; to improve throughput (we focus on read throughput currently), DatenLord, on one hand, prefetches data in memory to speed up access, on the other hand, leverages K8S to schedule tasks to data nearby, so as to minimize data transfer, like Hadoop does.

----

## Architecture

DatenLord is of a classical master-slave architecture. To achieve better storage performance, DatenLord has a coupled architecture with K8S, that DatenLord is deployed within a K8S cluster, in order to leverage data locality to speed up data access. Below is the overall DatenLord architecture, the green parts are DatenLord components, the blue parts are K8S components, the yellow part represents containerized applications, the XXX part represents kernel modules. There are several major components of DatenLord: master node (marked as Daten Lord), slave node (marked as Daten Sklavin), and K8S plugins.
![DatenLord Architecture](docs/images/Computing%20Defined%20Storage%402x.png "DatenLord Overall Architecture")

The master node has three parts: S3 compatible interface (S3I), Lord, and Meta Storage Engine (MSE). S3I provides a convenient way to read and write data in DatenLord via S3 protocal, especially for bulk upload and download senarios, e.g. uploading large amount of data for big data batch jobs or AI machine leanring training jobs. Lord is the overall controller of DatenLord, which controls all the interal behaviers of DatenLord, such as where and how to write data, synchronize data, etc. MSE stores all the meta information of DatenLord, such as the file pathes of all the data stored in each slave node, the user defined labels of each data file, etc. MSE is similar to HDFS namenode.

The slave node has four parts: Data Storage Engine (DSE), Sklavin, Meta Storage Engine (MSE), S3/P2P interface. DSE is in charge of communicate with kernel modules so as to read/write data from/to memory or disks. More specifically, DatenLord sets up a file system in user space (FUSE) in a slave node by using FUSE driver and library. DSE is to implement the FUSE, executing all the underlying FUSE operation, such as open, create, read, and write, etc. DSE can implement different types of FUSE for different senarios, e.g., memory-based FUSE, disk-based FUSE, and SAN-based FUSE, etc. Sklavin is to communite with the Lord of the master node and handle the requests from the Lord and CSI driver, such as health check report, data synchronization, data consistency inspection, Lord election, etc. The MSE of the slave node is a local copy of the MSE from the master node. S3/P2P interface provides a convenient way, either S3 or P2P, to read, write and synchronize data in a slave node.

The K8S plugins includes a container storage interface (CSI) driver and a customer filter. The CSI driver is for DatenLord to work with K8S to manage volumns for container tasks, such as loading a read-only volumn, creating a read-write volumn. The customer filter is to help K8S to schedule tasks to data nearby based on the meta information in MSE of the master node.

### Latency-Sensitive Senario

As for the latency-sensitive senario, like database application, the underlying assumption is that each I/O-request needs to be handled as soon as possible, whereas each I/O-request doesn't handle many data as big data batch job does.

Below is the architecture and workflow of DatenLord for latency-sensitive senario, where red solid lines represent K8S workflow and purple dotted lines represent DatenLord workflow.
![Latency-Sensitive Architecture](docs/images/Computing%20Defined%20Storage%20-%20Persistent%402x.png "Latency-Sensitive Architecture")

The work flow for latency-sensitive senario is as following. Initally, when K8S receives a task request, the K8S API server schedules a kubelet on a node to launch a container of this task. If this task requires one or more volumes, the kubelet will request the local DatenLord CSI driver to prepare the requested volumns. Then, the CSI driver forwards the volume request to the Sklavin of the same node. In the latency-sensitive senario, Sklavin creates volumns backed up by the memory-based FUSE. After volumns are ready, the kubelet will launch the container of this task. Once the container is launched and in runtime, it can read and write data through DatenLord volumns.

As the container accessing DatenLord volumns, the memory-based FUSE will cache in memory as much data as possible, which caches data based on the access pattern that warm data in memory and cold data on disk. Meanwhile, data modified in memory-based FUSE will be stored to disk asychronizedly. Furthermore, Sklavin will periodically generate checkpoints or snapshots for disaster recovery.

### Throughput-Sensitive Senario




## Road Map
