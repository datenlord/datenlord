# DatenLord

----
DatenLord is a cloud-native, applicated-orientated distributed storage system, especially for container ecosystem.
DatenLord provides high-performance storage service for containers, which facilitates state-ful applications running on top of K8S.

DatenLord is designed to serve the storage needs of two senarios, one is *latency-sensitive*, and the other is *throughput-sensitive*.
As for latency-sensitive tasks, such as ElasticSearch, MySQL, etc, their performance relies on how fast a single I/O-request got handled.
As for throughput-sensitive tasks, such as big-data and machine learning tasks, like Apache Spark, TensorFlow, etc, the more data read per unit time, the better performance.

DatenLord is crafted for the aforementioned two senarios.
Specifically, to reduce latency, DatenLord caches in memory as much data as possible, to minimize disk access; to improve throughput, DatenLord, on one hand, prefetches data in memory to speed up access, on the other hand, leverages K8S to schedule tasks to data nearby, so as to minimize data transfer, like Hadoop does.

----
