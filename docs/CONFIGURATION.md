**Table of Contents**
- [Examples](#examples)
- [Configuring Peer To Peer Download](#configuring-peer-to-peer-download)
  - [Tracker Peer TTL](#tracker-peer-ttl)
  - [Bandwidth](#bandwidth)
  - [Connection Limits](#connection-limits)
  - [Seeder TTI](#seeder-tti)
  - [Torrent TTI On Disk](#torrent-tti-on-disk)
- [Configuring Hash Ring](#configuring-hash-ring)
  - [Active Health Check](#active-health-check)
  - [Passive Health Check](#passive-health-check)
- [Configuring Storage Backend For Origin And Build-Index](#configuring-storage-backend-for-origin-and-build-index)
  - [Read-Only Registry Backend](#read-only-registry-backend)
  - [Bandwidth on Origin](#bandwidth-on-origin)

# Examples

Here are some example configuration files we used for dev cluster (which can be started by running
`make devcluster`).

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

Kraken's peer-to-peer network consists of agents, origins and trackers. Origins are special dedicated peers that seed data from a storage backend (HDFS, S3, etc). Agents are peers that download from each other and from origins. Agents periodically announce each torrent they are currently downloading to tracker, and in return, receive a list of peers that are also seeding the same torrent. More details in [ARCHITECTURE.md](ARCHITECTURE.md)

## Tracker Peer TTL

>tracker.yaml
>```yaml
>peerstore:
>   redis:
>     peer_set_window_size: 1h
>     max_peer_set_windows: 5
>```
As peers announce periodically to a tracker, the tracker stores the announce requests into several time window bucket.
Each announce request expires in `peer_set_window_size * max_peer_set_windows` time.

Then, the tracker returns a random set of peers selecting from `max_peer_set_windows` number of time bucket.

## Announce Interval `TODO(evelynl94)`

## Bandwidth

Download and upload bandwidths are configurable to prevent peers from saturating the host network.
>agent.yaml/origin.yaml
>```yaml
>scheduler:
>   conn:
>     bandwidth:
>       enable: true
>       egress_bits_per_sec: 1677721600  # 200*8 Mbit
>       ingress_bits_per_sec: 2516582400 # 300*8 Mbit
>```

## Connection Limits

Number of connections per torrent can be limited by:
>agent.yaml/origin.yaml
>```yaml
>scheduler:
>   connstate:
>     max_open_conn: 10
>```
There is no limit on number of torrents a peer can download simultaneously.

## Pipeline limit `TODO(evelynl94)`

## Seeder TTI

SeederTTI (time-to-idle) is the duration a completed torrent will exist without being read from before being removed from in-memory archive.
>agent.yaml/origin.yaml
>```yaml
>scheduler:
>   seeder_tti: 5m
>```
However, until it is deleted by periodic storage purge, completed torrents will remain on disk and can be re-opened on another peer's request.

## Torrent TTI On Disk

Both agents and origins can be configured to cleanup idle torrents on disk periodically.
>agent.yaml/origin.yaml
>```yaml
>store:
>   cache_cleanup:
>     tti: 6h
>   download_cleanup:
>     tti: 6h
>```

For origins, the number of files can also be limited as origins are dedicated seeders and hence normally caches files on disk for longer time.
>origin.yaml
>```yaml
>store:
>   capacity: 1000000
>
>```

# Configuring Hash Ring

Both orgin and tracker clusters are self-healing hash rings and both can be represented by either a dns name or a static list of hosts.

We use rendezvous hashing for constructing ring membership.

Take an origin cluster for example:
>origin-static-hosts.yaml
>```yaml
>hashring:
>   max_replica: 2
>cluster:
>   hosts:
>     static:
>     - origin1:15002
>     - origin2:15002
>     - origin3:15002
>```
>origin-dns.yaml
>```yaml
>hashring:
>   max_replica: 2
>cluster:
>   hosts:
>     dns: origin.example.com:15002
>```

## Health Check For Hash Rings

When a node in the hash ring is considered as unhealthy, the ring client will route requests to the next healthy node with the highest score. There are two ways to do health check:

### Active Health Check

Origins do health check for each other in the ring as the cluster is usually smaller.
>origin.yaml
>```yaml
>cluster:
>   healthcheck:
>     filter:
>       fails: 3
>       passes: 2
>     monitor:
>       interval: 30s
Above configures health check ping from one origin to others every 30 seconds. If 3 or more consecutive health checks fail for an origin, it is marked as unhealthy. Later, if 2 or more consecutive health checks succeed for the same origin, it is marked as healthy again. Initially, all hosts are healthy.

### Passive Health Check

Agents health checks tracker, piggybacking on the announce requests.
>agent.yaml
>```yaml
>tracker:
>   cluster:
>     healthcheck:
>       fails: 3
>       fail_timeout: 5m
>```
As shown in this example, if 3 announce requests to one tracker fail with network error within 5 minutes, the host is marked as unhealthy for 5 minutes. The agent will not send requests to this host until after timeout.

# Configuring Storage Backend For Origin And Build-Index

Storage backends are used by Origin and Build-Index for data persistence. Kraken has support for S3, GCS, ECR, HDFS, http (readonly), and Docker Registry (readonly) as [backends](https://github.com/uber/kraken/tree/master/lib/backend).

Multiple backends can be used at the name time, configured based on namespaces of requested blob and tag  (for docker images, that means the part of image name before ":").

Example origin config that uses multiple backends:

>origin.yaml
>```yaml
>backends:
> - namespace: library/.*
>    backend:
>      registry_blob:
>        address: index.docker.io
>        timeout: 60s
>        security:
>          basic:
>            username: ""
>            password: ""
> - namespace: test-domain/.*
>    backend:
>      http:
>        download_url: http://test-domain:9000/download?sha256=%s
>        download_backoff:
>          enabled: true
> - namespace: ecr-images/.*
>   backend:
>     registry_tag:
>       address: 123456789012.dkr.ecr.<region>.amazonaws.com
>       security:
>         credsStore: 'ecr-login'
> - namespace: s3-images/.*
>   backend:
>     s3:
>       region: us-west-1
>       bucket: test-bucket
>       root_directory: /test-bucket/kraken/default/
>       name_path: sharded_docker_blob
>       username: kraken-user
> - namespace: minio-images/.*
>   backend:
>     s3:
>       region: us-east-1
>       bucket: self-hosted-bucket
>       root_directory: /kraken/default/
>       name_path: sharded_docker_blob
>       username: minio-user
>       endpoint: http://172.17.0.1:9000
>       disable_ssl: true
>       force_path_style: true
>   bandwidth:
>     enable: true
> - namespace: gcs-images/.*
>   backend:
>     gcs:
>       username: kraken-user
>       bucket: test-bucket
>       root_directory: /test-bucket/kraken/default/
>       name_path: sharded_docker_blob
>   bandwidth:
>     enable: true
>
>auth:
>  s3:
>    kraken-user:
>      s3:
>        aws: kraken-user
>        aws_access_key_id: <keyid>
>        aws_secret_access_key: <key>
>    minio-user:
>      s3:
>        aws: minio-user
>        aws_access_key_id: <keyid>
>        aws_secret_access_key: <key>
>  gcs:
>    kraken-user:
>      gcs:
>        access_blob: <service_account_key>

## Read-Only Registry Backend

For simple local testing with an insecure registry (assuming it listens on `host.docker.internal:5000`), you can configure the backend for origin and build-index accordingly:

>origin.yaml
>```yaml
>backends:
>  - namespace: .*
>    backend:
>      registry_blob:
>        address: host.docker.internal:5000
>        security:
>          tls:
>            client:
>              disabled: true
>```

>build-index.yaml
>```yaml
>backends:
>  - namespace: .*
>    backend:
>      registry_tag:
>        address: host.docker.internal:5000
>        security:
>          tls:
>            client:
>              disabled: true
>```

## Bandwidth on Origin

When transferring data from and to its storage backend, origins can be configured with download and upload bandwidths. This is useful when using cloud storage providers to prevent origins from saturating the network link.
>origin.yaml
>```yaml
>backends:
>  - namespace: .*
>    backend:
>      s3: <omitted>
>    bandwidth:
>      enabled: true
>      egress_bits_per_sec: 8589934592   # 8 Gbit
>      ingress_bits_per_sec: 85899345920 # 10*8 Gbit
>```
