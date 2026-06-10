#!/bin/bash
# Phase 2 soci-snapshotter lazy-pull e2e benchmark -- run this FROM THE HOST.
#
# This is the single host entrypoint. It builds and starts the kraken-soci DinD
# container (containerd + soci-snapshotter + nerdctl/soci/ctr), waits for it to
# become healthy against the running devcluster, then drives the overlayfs-vs-soci
# comparison inside it via `docker exec` and prints the results here.
#
# Prerequisites: a running devcluster -- `make devcluster` -- which brings up the
# Kraken proxy (:15000) and agent registries (:16000, :17000).
#
# Usage:
#   examples/devcluster/soci/soci_benchmark.sh [source-image] [cmd...]
#
# Env:
#   REBUILD=1   force a rebuild of the kraken-soci:dev harness image
#   KEEP=1      leave the kraken-soci container running after the benchmark

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
SOCI_DIR="${REPO_ROOT}/examples/devcluster/soci"
CONTAINER=kraken-soci

IMAGE="${1:-public.ecr.aws/docker/library/python:3.12}"
shift || true
CMD=("$@")

log() { echo "[soci-bench] $*" >&2; }

# Build the harness image if missing or REBUILD=1.
if [ "${REBUILD:-0}" = "1" ] || ! docker image inspect kraken-soci:dev >/dev/null 2>&1; then
    log "building kraken-soci:dev"
    docker build -t kraken-soci:dev "${SOCI_DIR}"
fi

# (Re)start the DinD container with fresh, cold caches.
docker rm -fv "${CONTAINER}" >/dev/null 2>&1 || true
log "starting ${CONTAINER}"
"${SOCI_DIR}/soci_start_container.sh"

# Wait for containerd inside the container, surfacing logs if it dies.
log "waiting for containerd + soci-snapshotter"
deadline=$(( $(date +%s) + 60 ))
until docker exec "${CONTAINER}" ctr version >/dev/null 2>&1; do
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        log "ERROR: ${CONTAINER} exited during startup; last logs:"
        docker logs "${CONTAINER}" 2>&1 | tail -50 >&2
        exit 1
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
        log "ERROR: containerd not ready within 60s; last logs:"
        docker logs "${CONTAINER}" 2>&1 | tail -50 >&2
        exit 1
    fi
    sleep 1
done
log "container ready"

# Drive the e2e measurement inside the container.
docker exec "${CONTAINER}" /usr/local/bin/run_e2e.sh "${IMAGE}" "${CMD[@]}"

if [ "${KEEP:-0}" = "1" ]; then
    log "KEEP=1: leaving ${CONTAINER} running"
else
    docker rm -fv "${CONTAINER}" >/dev/null 2>&1 || true
fi
