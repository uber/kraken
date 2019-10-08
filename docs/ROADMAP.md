# Performance

Kraken's performance goal is to be capable of distributing a 100GB blob to 10k hosts concurrently at
80% of host nic limit.

Currently, Kraken works the best if blob is smaller than 10G, so some peers could become seeders
soon, and help reduce load on origin cluster.
If the blob is too big, peers joined later will spend extra effort to find openings in the network,
and the topology they form would not be very balanced, negatively impacting download speed.

To support extra large blobs, one possible solution is to let the cluster periodically rebalance
itself to converge to a random regular graph, which in theory could guarantee high download speed
for all participants.

# Security

Kraken supports bi-directional TLS between all components for image tags, and bi-directional TLS
between image builder and Kraken for all data.
Uploaders can be autherized; and since blobs are content addressable, data integrity is protected.
However, blob content in torrent traffic is not encrypted, so it's still vulnerable to eavesdropping.

We plan to support TLS on torrent traffic.

# Kubernetes Integration

We are looking into integrating Kraken with Kubernetes, so Kraken can preheat hosts at the beginning
of rolling upgrades, and speed up deployments.

This would require a Kubernetes scheduler that supports in-place upgrade.

# Tag Mutation

Mutating tags is allowed, but the behavior is undefined. Replication probably won't trigger, and
most tag lookups could still return the old tag due to caching.

We plan to support tag mutation.

# Visualization

Current visualization tool is based on agent logs. If feasible, we hope to support realtime
visualization.

# BitTorrent Compatibility

Kraken's torrent library is based on a simplified version of BitTorrent, however it is not
compatible with the BitTorrent protocol. We may investigate BitTorrent compatibility in the future.
