# Kraken :octopus:

[![Build Status](https://travis-ci.org/uber/kraken.svg?branch=master)](https://travis-ci.org/uber/kraken)
[![Github Release](https://img.shields.io/github/release/uber/kraken.svg)](https://github.com/uber/kraken/releases)

Kraken is a P2P-powered Docker registry which focuses on scalability and availability.

Some highlights of Kraken:
- Highly scalable. Cluster size has no impact on download speed (largest Kraken cluster at Uber is
  8k hosts).
- Highly available. No component is a single point of failure.
- Pluggable storage options. Instead of managing data, Kraken plugs into reliable blob storage
  options, like S3 or HDFS. The storage interface is simple, and new options are easy to add.
- Lossless cross cluster replication. Kraken supports rule-based async replication between clusters.
- Minimal dependencies. Other than pluggable storage, Kraken only has an optional dependency on DNS.

Kraken has been in production at Uber since early 2018. In our busiest cluster, Kraken distributes
1 million 0-100MB blobs, 600k 100MB-1G blobs, and 100k 1G+ blobs per day. At its peak production
load, Kraken distributes 20K 100MB-1G blobs in under 30 sec.

- [Design](#design)
- [Architecture](#architecture)
- [Benchmark](#benchmark)
- [Usage](#usage)
- [Comparison With Other Projects](#comparison-with-other-projects)
- [Limitations](#limitations)

# Design

Visualization of a small Kraken cluster at work:

![](assets/visualization.gif)

The high level idea of Kraken is to have a small number of dedicated hosts seed content to a network
of agents running on each host in the cluster. Agents discover other peers through a central component
which tracks who is downloading what. Agents will prefer to download from other agents seeding the
desired content, whereas the dedicated seeders are only used by the first participants in the network to
kick things off. In practice, we use connection limits and randomization to form sparse graphs with
small diameter so that in large networks, agents are never far from a seeder.

Docker registry interfaces are plopped on top of each peer in the network, such that images are
pushed to the dedicated seeders and pulled from the agents.

At play are two core primitives: blobs and tags. Blobs are content-addressable, meaning each
blob is identified by the hash of its content (known as a *digest*). Tags map human readable names
to blob digests. Docker images are mapped into these primitives like so:
- The image manifest is a blob.
- Each layer referenced in the manifest is a blob.
- The image tag maps to a manifest digest.

Note, neither of these primitives is married exclusively to Docker registry. Non-Docker artifacts
can be tagged and distributed through Kraken's underlying API.

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
- p100 = 22s

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

For more details on how to configure Kraken, please refer to the [documentation](docs/CONFIGURATION.md).

# Comparison With Other Projects

## Dragonfly from Alibaba

https://github.com/dragonflyoss/Dragonfly

Comparison pending discussion with Dragonfly team.

# Limitations

- If Docker registry throughput is not the bottleneck in your deployment workflow, switching to
  Kraken will not magically speed up your `docker pull`. Instead, consider the lower hanging fruit
  of reducing your image sizes by building images with [Makisu](https://github.com/uber/makisu).
- Mutating tags is allowed, however the behavior is undefined. A few things will go wrong: replication
  probably won't trigger, and most tag lookups will probably still return the old tag due to caching.
  If you need mutation (e.g. updating a `latest` tag), please consider implementing your own index
  component on top of a consistent key-value store.
- Theoretically, Kraken should handle blobs of any size without significant performance degredation,
  but at Uber we enforce a 20G limit and cannot endorse of the production use of ultra-large blobs
  (i.e. 100G+). Kraken's garbage collection policies are TTL-based and quite simplistic. The primary
  risk is that your disk space across the cluster will rapidly fluctuate and cause shortages.
