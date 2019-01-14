**Table of Contents**
- [Examples](#examples)
- [Configuring Peer To Peer Download](#configuring-peer-to-peer-download)
  - [Tracker peer set TTI](#tracker-peer-set-tti)
  - [Bandwidth](#bandwidth)
  - [Connection limit](#connection-limit)
  - [Seeder TTI](#seeder-tti)
  - [Torrent TTI on disk](#torrent-tti-on-disk)
- [Configuring Hash Ring](#configuring-hash-ring)
  - [Active health check](#active-health-check)
  - [Passive health check](#passive-health-check)
- [Configuring Storage Backend Bandwidth on Origin](#configuring-storage-backend-bandwidth-on-origin)

# Examples

Here are some example configuration files we used for dev cluster (which can be started by running
'make devcluster').

They are split into a base.yaml that contains configs that we have been using for test, development
and production, and a development.yaml that contains configs specifically needed for starting dev
cluster using Docker-for-Mac, and need to updated for production setups.

- Origin
  - [base.yaml](../config/origin/base.yaml)
  - [development.yaml](../examples/devcluster/config/origin/development.yaml)

- Tracker
  - [base.yaml](../config/tracker/base.yaml)
  - [development.yaml](../examples/devcluster/config/tracker/development.yaml)

- Build-index
  - [base.yaml](../config/build-index/base.yaml)
  - [development.yaml](../examples/devcluster/config/build-index/development.yaml)

- Proxy
  - [base.yaml](../config/proxy/base.yaml)
  - [development.yaml](../examples/devcluster/config/proxy/development.yaml)

- Agent
  - [base.yaml](../config/agent/base.yaml)
  - [development.yaml](../examples/devcluster/config/agent/development.yaml)

More in [examples/devcluster/README.md](../examples/devcluster/README.md)

# Configuring Peer To Peer Download

Kraken's peer-to-peer network consists of agents, origins and trackers. Origins are a few dedicated seeders that downloads data from a storage backend (HDFS, S3, etc). Agents are leechers that download from each other and from origins and can later become seeders after they finish downloading. Agents announce to trackers periodically to update the torrent they are currently downloading and in return get a list of peers that are also downloading the same torrent. More details in [ARCHITECTURE.md](ARCHITECTURE.md)

## Tracker peer set TTI

>tracker.yaml
>```
>peerstore:
>    redis:
>        peer_set_window_size: 1h
>        max_peer_set_windows: 5
>```
As peers announce periodically to a tracker, the tracker stores the announce requests into several time window bucket.
Each announce request expires in `peer_set_window_size * max_peer_set_windows` time.

Then, the tracker returns a random set of peers selecting from `max_peer_set_windows` number of time bucket.

## Announce interval `TODO(evelynl94)`

## Bandwidth

Download and upload bandwidths are configurable, avoiding peers to saturate the host network.
>agent.yaml/origin.yaml
>```
>scheduler:
>    conn:
>        bandwidth:
>            enable: true
>            egress_bits_per_sec: 1677721600  # 200*8 Mbit
>            ingress_bits_per_sec: 2516582400 # 300*8 Mbit
>```

## Connection limit

Number of connections per torrent can be limited by:
>agent.yaml/origin.yaml
>```
>scheduler:
>   connstate:
>       max_open_conn: 10
>```
There is no limit on number of torrents a peer can download simultaneously.

## Pipeline limit `TODO(evelynl94)`

## Seeder TTI

SeederTTI is the duration a completed torrent will exist without being read from before being removed from in-memory archive.
>agent.yaml/origin.yaml
>```
>scheduler:
>    seeder_tti: 5m
>```
However, until it is deleted by periodic storage purge, completed torrents will remain on disk and can be re-opened on another peer's request.

## Torrent TTI on disk

Both agents and origins can be configured to cleanup idle torrents on disk periodically.
>agent.yaml/origin.yaml
>```
>store:
>    cache_cleanup:
>        disabled: false
>        tti: 1h
>    download_cleanup:
>        disabled: false
>        tti: 1h
>```

For origins, the number of files can also be limited as origins are dedicated seeders and hence normally caches files on disk for longer time.
>origin.yaml
>```
>store:
>    capacity: 1000000
>
>```

# Configuring Hash Ring

Both orgin and tracker clusters are self-healing hash rings and both can be represented by either a dns name or a static list of hosts.

We use redenzvous hashing for constructing ring membership.

Take an origin cluster for example:
>origin-static-hosts.yaml
>```
>hashring:
>   max_replica: 2
>cluster:
>   hosts:
>       static:
>       - origin1:15002
>       - origin2:15002
>       - origin3:15002
>```
>origin-dns.yaml
>```
>hashring:
>   max_replica: 2
>cluster:
>   hosts:
>       dns: origin.example.com:15002
>```

## Health check for hash rings

When a node in the hash ring is considered as unhealthy, the ring client will route requests to the next healthy node with the highest score. There are two ways to do health check:

### Active health check

Origins do health check for each other in the ring as the cluster is usually smaller.
>origin.yaml
>```
>cluster:
>   healthcheck:
>       filter:
>           fails: 3
>           passes: 2
>       monitor:
>           interval: 30s
Above configures health check ping from one origin to others every 30 seconds. If 3 or more consecutive health checkes fail for an origin, it is marked as unhealthy. Later, if 2 or more consecutive health checks succeed for the same origin, it is marked as healthy again. Initially, all hosts are healthy.

### Passive health check

Agents health checks tracker, piggybacking on the announce requests.
>agent.yaml
>```
>tracker:
>   cluster:
>       healthcheck:
>           fails: 3
>           fail_timeout: 5m
>```
As shown in this example, if 3 or more consecutive announce requests to one tracker fail with network error, the host is marked as unhealthy for 5 minutes. The agent will not send requests to this host until after timeout.

# Configuring Storage Backend Bandwidth on Origin

When transfering data from and to its storage backend, origins can be configured with download and upload bandwidthes. Specially if you are using a cloud storage provider, this is helpful to prevent origins from saturating the network link.
>origin.yaml
>```
>backends:
>   - namespace: .*
>     bandwidth:
>         enabled: true
>         egress_bits_per_sec: 8589934592   # 8 Gbit
>         ingress_bits_per_sec: 85899345920 # 10*8 Gbit
>```
