# Kraken :octopus:

[![Build Status](https://travis-ci.org/uber/kraken.svg?branch=master)](https://travis-ci.org/uber/kraken)
[![Github Release](https://img.shields.io/github/release/uber/kraken.svg)](https://github.com/uber/kraken/releases)

Kraken is highly scalable P2P blob distribution system for large docker images and content addressable blobs.

Some highlights of Kraken:
* Highly scalable. It's capable of distributing docker images at speed of 1TB/sec with ease, and image size doesn't impact download speed. It supports 8k host clusters without problem.
* Highly available. Kraken cluster would remain operational as long as one Kraken host still works.
* Secure. Supports bi-directional TLS between all components for image tags, and bi-directional TLS between image builder and Kraken for all data (if your image builder supports client-side TLS, like [Makisu](https://github.com/uber/makisu)).
* Pluggable. It supports using S3/HDFS as storage backend, and it's easy to add more storage drivers.
* Lossless cross cluster replication. Kraken supports async replication between clusters based on namespace and repo name.

# Artitecture

Visualization of a small Kraken cluster at work:
![](assets/visualization.gif)

** Kraken Core


** Kraken Index
