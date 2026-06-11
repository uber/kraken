#!/bin/bash
# eStargz (stargz-snapshotter) lazy-pull e2e benchmark -- run this FROM THE HOST.
# Format-agnostic counterpart to soci_benchmark.sh.
#
# This is the single host entrypoint. It builds and starts the kraken-estargz
# DinD container (containerd + containerd-stargz-grpc + nerdctl/ctr-remote/ctr),
# which runs the overlayfs-vs-stargz comparison (run_e2e.sh) as its own workload.
# The host streams the results via `docker logs -f` -- deliberately NOT
# `docker exec`, which needs an HTTP->TCP connection upgrade that the busy daemon
# transiently refuses with "unable to upgrade to tcp, received 200". `docker logs`
# is a plain stream.
#
# Prerequisites: a running devcluster -- `make devcluster` -- which brings up the
# Kraken proxy (:15000) and agent registries (:16000, :17000).
#
# Usage:
#   examples/devcluster/estargz/estargz_benchmark.sh [source-image] [cmd...]
#
# Env:
#   REBUILD=1   force a rebuild of the kraken-estargz:dev harness image
#   KEEP=1      leave the kraken-estargz container running after the benchmark

set -euo pipefail

make devcluster

REPO_ROOT="$(git rev-parse --show-toplevel)"
ESTARGZ_DIR="${REPO_ROOT}/examples/devcluster/estargz"
CONTAINER=kraken-estargz

IMAGE="${1:-public.ecr.aws/docker/library/python:3.12}"
shift || true
CMD=("$@")

log() { echo "[estargz-bench] $*" >&2; }

# Build the harness image if missing or REBUILD=1.
if [ "${REBUILD:-0}" = "1" ] || ! docker image inspect kraken-estargz:dev >/dev/null 2>&1; then
    log "building kraken-estargz:dev"
    docker build -t kraken-estargz:dev "${ESTARGZ_DIR}"
fi

# blob_stats <container> <start-line> <status> -> "<count> blob GETs, <MiB> MiB"
blob_stats() {
    docker logs "$1" 2>/dev/null | tail -n +"$(( $2 + 1 ))" \
        | awk -v st=" $3 " '$0 ~ "/blobs/" && index($0, st) {c++; b+=$(NF-2)}
            END {printf "%d blob GETs, %.1f MiB", c+0, (b+0)/1048576}'
}

# Snapshot the agent access-log line counts BEFORE the run so we can attribute
# only THIS run's blob fetches. The stargz (lazy) leg hits agent-two with 206
# ranged GETs; the overlayfs (full) leg hits agent-one with 200 GETs. Comparing
# bytes fetched is the clearest lazy proof -- stargz should transfer far less
# than the full image. Agent log line: '... " 206 <bytes> "" "<ua>"' (bytes = NF-2).
log_lines() { docker logs "$1" 2>/dev/null | wc -l | tr -d ' '; }
a1_start=$(log_lines kraken-agent-one)
a2_start=$(log_lines kraken-agent-two)

# (Re)start the DinD container, passing the image + command so its entrypoint
# runs the benchmark and writes the full report to stdout (== docker logs).
docker rm -fv "${CONTAINER}" >/dev/null 2>&1 || true
log "starting ${CONTAINER} (runs the benchmark as its workload)"
"${ESTARGZ_DIR}/estargz_start_container.sh" "${IMAGE}" "${CMD[@]}"

# Stream the in-container report live. `docker logs -f` returns when the
# container exits; `docker wait` then yields run_e2e's exit code. Neither needs
# a connection upgrade, so this path is immune to the exec hijack flake.
log "streaming benchmark output"
docker logs -f "${CONTAINER}" 2>&1 || true
rc=$(docker wait "${CONTAINER}" 2>/dev/null || echo 1)

# Bytes fetched from the Kraken agents during this run (host-side proof).
echo
echo "==================== bytes fetched from Kraken agents ===============" >&2
echo "stargz    (lazy, agent-two 206): $(blob_stats kraken-agent-two "${a2_start}" 206)" >&2
echo "overlayfs (full, agent-one 200): $(blob_stats kraken-agent-one "${a1_start}" 200)" >&2
echo "stargz should be FAR below overlayfs; if not, the layer was read in full" >&2
echo "====================================================================" >&2

if [ "${KEEP:-0}" = "1" ]; then
    log "KEEP=1: leaving ${CONTAINER} in place"
else
    docker rm -fv "${CONTAINER}" >/dev/null 2>&1 || true
fi

[ "${rc}" = "0" ] || { log "benchmark exited non-zero (${rc})"; exit "${rc}"; }
