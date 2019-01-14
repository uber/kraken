# Kraken :octopus:

[![Build Status](https://travis-ci.org/uber/kraken.svg?branch=master)](https://travis-ci.org/uber/kraken)
[![Github Release](https://img.shields.io/github/release/uber/kraken.svg)](https://github.com/uber/kraken/releases)

Kraken is a P2P-powered Docker registry which focuses on scalability and availability.

# Features

Following are some highlights of Kraken:
- **Highly scalable**. Kraken is capable of distributing docker images at > 50% of max download
  speed limit on every host. Cluster size and image size do not have significant impact on download
  speed.
  - Supports at least 8k hosts per cluster.
  - Supports arbitrarily large blobs/layers. We normally limit max size to 20G for best performance.
- **Highly available**. No component is a single point of failure.
- **Secure**. Support uploader authentication and data integrity protection through TLS.
- **Pluggable storage options**. Instead of managing data, Kraken plugs into reliable blob storage
  options, like S3, HDFS or another registry. The storage interface is simple, and new options
  are easy to add.
- **Lossless cross cluster replication**. Kraken supports rule-based async replication between
  clusters.
- **Minimal dependencies**. Other than pluggable storage, Kraken only has an optional dependency on
  DNS.

Kraken has been in production at Uber since early 2018. In our busiest cluster, Kraken distributes
1 million 0-100MB blobs, 600k 100MB-1G blobs, and 100k 1G+ blobs per day. At its peak production
load, Kraken distributes 20K 100MB-1G blobs in under 30 sec.

- [Design](#design)
- [Architecture](#architecture)
- [Benchmark](#benchmark)
- [Usage](#usage)
- [Comparison With Other Projects](#comparison-with-other-projects)
- [Limitations](#limitations)
- [Contributing](#contributing)
- [Contact](#contact)

# Design
Visualization of a small Kraken cluster at work:
![](assets/visualization.gif)

The high level idea of Kraken is to have a small number of dedicated hosts seed content to a network
of agents running on each host in the cluster.
A central component, tracker, will orchestrate all participants in the network to form a
pseudo-random regular graph.
Such a graph has high connectivity and small diameter, so all participants in a reasonally sized
cluster can reach > 80% of max upload/download speed in theory, and performance doesn't degrade much
as the blob size and cluster size increase.

# Architecture

![](assets/architecture.svg)

- Agent
  - Deployed on every host
  - Implements Docker registry interface
  - Announces available content to tracker
  - Connects to peers returned by tracker to download content
- Origin
  - Dedicated seeders
  - Stores blobs as files on disk backed by pluggable storage (e.g. S3)
  - Forms a self-healing hash ring to distribute load
- Tracker
  - Tracks which peers have what content (both in-progress and completed)
  - Provides ordered lists of peers to connect to for any given blob
- Proxy
  - Implements Docker registry interface
  - Uploads each image layer to the responsible origin (remember, origins form a hash ring)
  - Uploads tags to build-index
- Build-Index
  - Mapping of human readable tag to blob digest
  - No consistency guarantees: client should use unique tags
  - Powers image replication between clusters (simple duplicated queues with retry)
  - Stores tags as files on disk backed by pluggable storage (e.g. S3)

# Benchmark

The following data is from a test where a 3G Docker image with 2 layers is downloaded by 2600 hosts
concurrently (5200 blob downloads), with 300MB/s speed limit on all agents (using 5 trackers and
5 origins):

![](assets/benchmark.svg)

- p50 = 10s (at speed limit)
- p99 = 18s
- p99.9 = 22s

# Usage

All Kraken components can be deployed as Docker containers. To build the Docker images:

```
$ make images
```

To start a herd container (which contains origin, tracker, build-index and proxy) and two agent
containers with development configuration:

```
$ make devcluster
```

For more information on devcluster, please check out devcluster [README](examples/devcluster/README.md)
For information about how to configure and use Kraken, please refer to the [documentation](docs/CONFIGURATION.md)

# Comparison With Other Projects

## Dragonfly from Alibaba

Dragonfly cluster has one or a few "supernodes" that coordinates transfer of every 4MB chunk of data
in the cluster.
While the supernode would be able to make optimial decisions, the throughput of the whole cluster is
limited by the processing power of one or a few hosts, and the performance would degrade linearly as
either blob size or cluster size increases.
Kraken's tracker only helps orchestrate the connection graph, and leaves negotiation of actual data
tranfer to individual peers, so Kraken scales better with large blobs.

On top of that, Kraken is HA and supports cross cluster replication, both are required for a
reliable hybrid cloud setup.

# Limitations

- If Docker registry throughput is not the bottleneck in your deployment workflow, switching to
Kraken will not magically speed up your `docker pull`. To actually speed up `docker pull`, consider
switching to [Makisu](https://github.com/uber/makisu) to improve layer reusability at build time, or
tweak compression ratios, as `docker pull` spends most of the time on data decompression.
- Mutating tags is allowed, however the behavior is undefined. A few things will go wrong:
replication probably won't trigger, and most tag lookups will probably still return the old tag due
to caching. We are working supporting this functionality better. If you need mutation (e.g. updating
a latest tag) right now, please consider implementing your own index component on top of a
consistent key-value store.
- Theoretically, Kraken should distribute blobs of any size without significant performance
degredation, but at Uber we enforce a 20G limit and cannot endorse of the production use of
ultra-large blobs (i.e. 100G+). Peers enforce connection limits on a per blob basis, and new peers
might be starved for connections if no peers become seeders relatively soon. If you have ultra-large
blobs you'd like to distribute, we recommend breaking them into <10G chunks first.

# Contributing

Please check out our [guide](docs/CONTRIBUTING.md).

# Contact

To contact us, please join our [Slack channel](https://join.slack.com/t/uber-container-tools/shared_invite/enQtNTIxODAwMDEzNjM1LWIyNzUyMTk3NTAzZGY0MDkzMzQ1YTlmMTUwZmIwNDk3YTA0ZjZjZGRhMTM2NzI0OGM3OGNjMDZiZTI2ZTY5NWY).
