# Kraken :octopus:

[![Build Status](https://travis-ci.org/uber/kraken.svg?branch=master)](https://travis-ci.org/uber/kraken)
[![Github Release](https://img.shields.io/github/release/uber/kraken.svg)](https://github.com/uber/kraken/releases)

Kraken is a highly scalable P2P blob distribution system for large docker images and content addressable blobs.

Some highlights of Kraken:
- Highly scalable. It's capable of distributing docker images at > 50% of max download speed limit (configurable) on every host; Cluster size and image size do not have significant impact on download speed.
  - Supports at least 8k hosts per cluster.
  - Supports arbitrarily large blobs/layers. We normally limit blob size to 20G to avoid storage space fluctuation.
- Highly available. Kraken cluster would remain operational even if mutiple origin hosts were lost at the same time.
- Secure. Supports bi-directional TLS between all components for image tags, and bi-directional TLS between image builder and Kraken for all data.
- Pluggable storage options. It supports S3/HDFS as storage backend, and it's easy to add more options. It can be setup as write-back cache with configurable TTL, so it can survive S3 outages without impacting functionality.
- Lossless cross cluster replication. Kraken supports rule-based async replication between clusters.
- Minimal dependency. Other than plugable storage, Kraken only has optional dependency on DNS.

Uber has been using Kraken in production since early 2018. In our busiest cluster, Kraken distributes 1 million 0-100MB blobs, 600k 100MB-1G blobs, and 100k 1G+ blobs per day. At its peak production load, Kraken distributes 20K 100MB-1G blobs under 30 sec with ease.

- [Design](#design)
- [Architecture](#architecture)
- [Benchmark](#benchmark)
- [Usage](#usage)
- [Comparison With Other Projects](#comparison-with-other-projects)
- [Limitations](#limitations)

# Design
Visualization of a small Kraken cluster at work:
![](assets/visualization.gif)

The high level idea of Kraken, is to have a 3~5 node dedicated seeder cluster (origin) backed by S3/GCS/HDFS, and a agent with docker registry interface on every host, then let a central component (tracker) instruct seeders and agents to form a pseudo-random regular graph. Such a graph has high connectivity and small diameter, so all participants in a reasonally sized cluster can reach > 75% of max upload/download speed in theory, and performance doesn't degrade much as the blob size and cluster size increase.

# Architecture

Kraken have multiple components, they are divided into components that's dedicated to P2P distribution of content addressable blobs within a cluster, and components that's used for docker image upload and cross cluster replication.

## Kraken Core

Central P2P components that's not specific to docker images:

![](assets/kraken_core.svg)

- Agent
  - Deployed on every host
  - Implements Docker registry interface
- Origin
  - Dedicated seeders
  - Pluggable storage backend (e.g. S3)
  - Self-healing hash ring
- Tracker
  - Tracks peers and seeders, instructs them to form a sparse graph
  - Self-healing hash ring

## Kraken Proxy and Build Index

Components responsible for image tags and replication to other clusters:

![](assets/kraken_build_index.svg)

- Proxy
  - Handles image upload and direct download
- Build Index
  - Mapping of human readable tag to blob hash (digest)
  - No consistency guarantees, client should use unique tags
  - Powers image replication between clusters. Simple duplicated queues with retry
  - Pluggable storage
  - Self-healing hash ring

# Benchmark

Following data is from a test where a 3G docker image with 2 layers is downloaded by 2600 hosts concurrently (5200 blob downloads), with 300MB/s speed limit on all agents (using 5 trackers and 5 origins):

![](assets/kraken_benchmark.svg)

- p50 = 10s (At speed limit)
- p99 = 18s
- p99.9 = 22s

# Usage

All Kraken components can be deployed as docker containers.
To build the Docker images, run:
```
make images
```
To start a herd container (which contains origin, tracker, build-index and proxy) and two agent containers with development configs, run:
```
make devcluster
```
For more details on how to configure Kraken, please refer to the [documentation](docs/CONFIGURATION.md).

# Comparison With Other Projects

## Dragonfly from Alibaba

Dragonfly cluster has one or a few "supernodes" that coordinates transfer of every 4MB chunk of data in the cluster. This is a design we considered in the very beginning of Kraken and decided not to use after doing some math. While the supernode would be able to make optimial decisions, the throughput of the whole cluster is limited by the processing power of one or a few hosts, and the performance would degrade linearly as either blob size or cluster size increases. Kraken's tracker only helps orchestrate the connection graph, and leaves negotiation of actual data tranfer to individual peers, so Kraken scales much better with large cluster and large blobs.

On top of that, Kraken is HA, it won't be affected by individual machine failures. Dragonfly doesn't seem to be HA.

## LAD from Facebook

LAD is used for P2P configuration deployment at Facebook. It constructs a distribution tree based on network topology in Facebook's datacenters. Uber's datacenters have very low network oversubscription ratio, so there is no need to consider network topology, a global upload/download speed limit would suffice.

Besides, in a tree topology, the download speed of a child node would be a fraction of its parent's upload speed. In Kraken's network, all nodes can download at max speed, no single node would become the bottleneck.

# Limitations

- If docker registry throughput wasn't the bottleneck in your deployment workflow, switching to Kraken wouldn't magically speed up your `docker pull`, because docker spends most of the time on data decompression. To actually speed up `docker pull`, consider switching to [Makisu](https://github.com/uber/makisu) to tweak compression ratio at build time, and then use Kraken to distribute uncompressed images, at the cost of additional IO.
- Kraken's cross cluster replication mechanism cannot handle tag mutation (handling that properly would require a globally consistent key-value store). Updating an existing tag (like `latest`) will not trigger replication. If that's required, please consider implementing your own index component on top of your prefered key-value store solution.
- Kraken is supposed to work with blobs of any size, and download speed wouldn't be impacted by blob size. However, as blobs grow bigger, GC and replication gets more expensive too, and could cause disk space fluctuation in origin cluster. In practice it's better to split extra large blobs into < 10G chunks.
