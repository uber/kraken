# Kraken :octopus:

[![Build Status](https://travis-ci.org/uber/kraken.svg?branch=master)](https://travis-ci.org/uber/kraken)
[![Github Release](https://img.shields.io/github/release/uber/kraken.svg)](https://github.com/uber/kraken/releases)

Kraken is highly scalable P2P blob distribution system for large docker images and content addressable blobs.
Some highlights of kraken are:
  - Highly scalable. It's capable of distributing docker images at speed of 1T/sec with ease, and image size doesn't impact download speed. It works with a 8k host cluster without problem.
  - Highly available. Kraken cluster would remain operational as long as one Kraken host still works.
  - Secure. Supports bi-directional TLS between all components for image tags, and bi-directional TLS between image pusher and kraken.
  - Pluggable. Supports using S3/HDFS as storage backend, and it's easy to add more storage drivers.
  - Lossless cross cluster replication. Kraken supports async replication between clusters based on namespace and repo name.
