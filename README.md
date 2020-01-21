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

DatenLord is of a classical master-slave architecture. Below is the overall DatenLord architecture.
[DatenLord Architecture](docs/images/Computing%20Defined%20Storage%402x.png)

There are several major components of DatenLord: master node (shown as Daten Lord, or Lord for short), slave node (shown as Daten Sklavin, or Sklavin[^1] for short), and K8S plugins.






[^1]: Sklavin is slave in German.
