#!/bin/bash
# Starts the nydus e2e DinD container. Requires a running `make devcluster`
# (Kraken proxy :15000, agent registries :16000 and :17000 on the host).
# Prefer driving this via the host script `nydus_benchmark.sh`.

set -ex

CONTAINER_NAME=kraken-nydus

# --privileged is required for containerd to mount overlay/fuse filesystems and
# for nydusd to create its FUSE mount.
#
# --network host lets the container address the devcluster agents as 127.0.0.1,
# so both containerd's resolver and nydusd's registry backend use plain HTTP
# against Kraken's plain-HTTP registry.
#
# The containerd and nydus data roots are placed on anonymous Docker volumes
# (real host fs) instead of the container's overlay2 rootfs: overlayfs cannot use
# an upperdir that itself lives on overlayfs, which otherwise fails snapshot
# mounts with "failed to mount overlay: invalid argument". Anonymous volumes are
# fresh per container, preserving a cold cache for each run.
#
# Any args ("$@") are passed through to the entrypoint as the benchmark image +
# command; with args the container runs run_e2e.sh and exits, with none it idles.
docker run -d \
    --privileged \
    --network host \
    -v /var/lib/containerd \
    -v /var/lib/nydus \
    --name ${CONTAINER_NAME} \
    kraken-nydus:dev "$@"
