# DatenLord

![Logo of DatenLord](docs/images/logo.png)

----
DatenLord is an application-orientated, cloud-native distributed storage system. Firstly, DatenLord is designed as an application-orientated storage system, which we call "Computing Defined Storage" in that DatenLord is optimized for many application senarios, such as database, big data, AI machine leanring, etc. Secondly, DatenLord is desinged as a cloud-native storage system, which works perfectly with cloud-native applications, especially container ecosystem. DatenLord provides high-performance storage service for containers, which facilitates stateful applications running on top of Kubernetes (K8S).

In general, there are two kinds of storage needs from application perspective: one is *latency-sensitive*, and the other is *throughput-sensitive*.
As for latency-sensitive applications, such as MySQL, ElasticSearch, etc, their performance relies on how fast a single I/O-request got handled.
As for throughput-sensitive applications, such as big-data, AI machine learning, like Apache Spark, TensorFlow, etc, the more data read per unit time, the better performance.

DatenLord is crafted to fit the aforementioned two senarios.
Specifically, to reduce latency, DatenLord caches in memory as much data as possible, in order to minimize disk access; to improve throughput, DatenLord, on one hand, prefetches data in memory to speed up access, on the other hand, leverages K8S to schedule tasks to data nearby, so as to minimize data transfer, like Hadoop does.

----

## DatenLord Architecture

DatenLord is of a classical master-slave architecture. Below is the overall DatenLord architecture, the green parts are DatenLord components, the blue parts are K8S components, the yellow part represents containerized applications, the XXX part represents kernel modules. There are several major components (shown as green parts in the architecture) of DatenLord: master node (marked as Daten Lord), slave node (marked as Daten Sklavin[^skl]), and K8S plugins.
![DatenLord Architecture](docs/images/Computing%20Defined%20Storage%402x.png)

The master node has three parts: S3 compatible interface (S3I), Lord, and Meta Storage Engine (MSE). S3I provides a convenient way to read and write data stored in DatenLord via S3 protocal, especially for bulk upload and download senarios, such as uploading large amount of data for big data batch jobs or AI machine leanring training jobs. Lord is the overall controller of DatenLord, which controls all the interal behaviers of DatenLord, such as where and how to write data, synchronize data, etc. MSE is to store all the meta information of DatenLord, such as the file pathes of all the data stored in each slave node, the user defined labels of each data file, etc.

The slave node has multiple parts: Data Storage Engine (DSE), Sklavin, Meta Storage Engine (MSE), S3/P2P interface. DSE is in charge of communicate with kernel modules so as to read/write data from/to memory or disks. Sklavin is to communite with the Lord of the master node and execute the command from the Lord, such as health check report, data synchronization, data consistency inspection, Lord election, etc. The MSE of the slave node is a local copy of the MSE from the master node. S3/P2P interface provides a convenient way, S3 or P2P, to read, write and synchronize data stored in a slave node.

The K8S plugins includes a container storage interface (CSI) driver and a customer filter. The CSI driver is for DatenLord to work with K8S to manage the storage for container tasks, such as creating a volumn for containers to write, loading a volumn for containers to read. The customer filter is to help K8S to schedule tasks to data nearby.







[^skl]: Sklavin is slave in German.
