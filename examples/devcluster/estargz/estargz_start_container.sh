#!/bin/bash
# Starts the estargz e2e DinD container. Requires a running `make devcluster`
# (Kraken proxy :15000, agent registries :16000 and :17000 on the host).
# Prefer driving this via the host script `estargz_benchmark.sh`.

set -ex

CONTAINER_NAME=kraken-estargz

# --privileged is required for containerd to mount overlay/fuse filesystems.
#
# --network host lets the container address the devcluster agents as 127.0.0.1,
# so stargz's resolver (insecure mirror) and containerd's own resolver both use
# plain HTTP against Kraken's plain-HTTP registry.
#
# The containerd and stargz data roots are placed on anonymous Docker volumes
# (real host fs) instead of the container's overlay2 rootfs: overlayfs cannot
# use an upperdir that itself lives on overlayfs, which otherwise fails snapshot
# mounts with "failed to mount overlay: invalid argument". Anonymous volumes are
# fresh per container, preserving a cold cache for each run.
#
# Any args ("$@") are passed through to the entrypoint as the benchmark image +
# command; with args the container runs run_e2e.sh and exits, with none it idles.
#
# COLD_AGENT is forwarded so run_e2e.sh can optionally cold agent-one (the
# overlay seeder) before the stargz leg, forcing pieces through the cold origin.
# COLD_OVERLAY is forwarded so the overlay leg can itself be served by a cold
# origin (cold both agents first) for the full-pull backend-egress baseline.
docker run -d \
    --privileged \
    --network host \
    -e COLD_AGENT="${COLD_AGENT:-0}" \
    -e COLD_OVERLAY="${COLD_OVERLAY:-0}" \
    -v /var/lib/containerd \
    -v /var/lib/containerd-stargz-grpc \
    --name ${CONTAINER_NAME} \
    kraken-estargz:dev "$@"
