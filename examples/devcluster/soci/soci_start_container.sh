#!/bin/bash
# Starts the soci e2e DinD container. Requires a running `make devcluster`
# (Kraken proxy :15000, agent registries :16000 and :17000 on the host).
# Prefer driving this via the host script `soci_benchmark.sh`.

set -ex

CONTAINER_NAME=kraken-soci

# --privileged is required for containerd to mount overlay/fuse filesystems.
#
# The containerd and soci data roots are placed on anonymous Docker volumes
# (real host fs) instead of the container's overlay2 rootfs: overlayfs cannot
# use an upperdir that itself lives on overlayfs, which otherwise fails snapshot
# mounts with "failed to mount overlay: invalid argument". Anonymous volumes are
# fresh per container, preserving a cold cache for each run.
docker run -d \
    --privileged \
    --add-host host.docker.internal:host-gateway \
    -v /var/lib/containerd \
    -v /var/lib/soci-snapshotter-grpc \
    --name ${CONTAINER_NAME} \
    kraken-soci:dev
