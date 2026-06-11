#!/bin/bash
# Starts the soci e2e DinD container. Requires a running `make devcluster`
# (Kraken proxy :15000, agent registries :16000 and :17000 on the host).
# Prefer driving this via the host script `soci_benchmark.sh`.

set -ex

CONTAINER_NAME=kraken-soci

# --privileged is required for containerd to mount overlay/fuse filesystems.
#
# --network host lets the container address the devcluster agents as 127.0.0.1.
# This is load-bearing, not cosmetic: soci-snapshotter's SOCI-artifact fetch
# (fs/artifact_fetcher.go) hardcodes PlainHTTP = MatchLocalhost(host) and ignores
# the resolver's `insecure` mirror config, so any non-localhost host forces HTTPS
# against Kraken's plain-HTTP registry and the index fetch fails. Addressing the
# agents as 127.0.0.1 makes every soci path (artifact fetch + layer Range reads)
# and containerd's own resolver use plain HTTP automatically.
#
# The containerd and soci data roots are placed on anonymous Docker volumes
# (real host fs) instead of the container's overlay2 rootfs: overlayfs cannot
# use an upperdir that itself lives on overlayfs, which otherwise fails snapshot
# mounts with "failed to mount overlay: invalid argument". Anonymous volumes are
# fresh per container, preserving a cold cache for each run.
#
# Any args ("$@") are passed through to the entrypoint as the benchmark image +
# command; with args the container runs run_e2e.sh and exits, with none it idles.
docker run -d \
    --privileged \
    --network host \
    -v /var/lib/containerd \
    -v /var/lib/soci-snapshotter-grpc \
    --name ${CONTAINER_NAME} \
    kraken-soci:dev "$@"
