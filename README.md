# WarpStream - One Billion Row Challenge

We've used a few tricks to achieve a processing time of ~10.5 seconds on an M1 MacBook Pro:

* Separating the aggregate state from the aggregate keys for better cache efficiency and less copying
* Parallel processing with many (tested 64, 128, and 256) small tasks by splitting the file into ranges
* Very little copying of the input data

_The output format is slightly different from the requested output format to enable easier comparison with other systems like ClickHouse._

---

## What is WarpStream?

WarpStream is an Apache Kafka protocol-compatible data streaming and storage system that stores data directly in object storage like Amazon S3 to:

* eliminate the inter-zone networking cost of replication in Apache Kafka and similar systems
* use the cheapest and most durable storage in the cloud (object storage) instead of local disks that are expensive
* allow you to scale up _and down_ on-demand using stateless compute instead of scaling your stateful Apache Kafka cluster for peak loads

check it out at [WarpStream.com](https://warpstream.com)
