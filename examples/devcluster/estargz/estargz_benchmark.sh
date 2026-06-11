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

if [ "${SKIP_CLUSTER:-0}" = "1" ]; then
    echo "[estargz-bench] SKIP_CLUSTER=1: reusing the running devcluster" >&2
else
    make devcluster
fi

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

# Snapshot agent-two's network-event log too, for the agent<->agent P2P tally
# over the stargz lazy leg. The overlayfs leg makes agent-one a full seeder of
# the converted image's blobs; the stargz leg then streams the SAME content-
# addressed blobs on agent-two, which can source pieces peer->peer from agent-one
# or from the origin. receive_piece events (peer = source) need network_event
# enabled in config/agent/development.yaml. Missing log => P2P tally is skipped.
NETLOG=/var/log/kraken/kraken-agent/networkevent.log
NET_TMP="$(mktemp -d)"
trap 'rm -rf "${NET_TMP}"' EXIT
# Copy an agent's network-event log to a host file via `docker cp` (the archive
# API is a plain HTTP GET, immune to the exec TCP-upgrade flake the busy daemon
# rejects with "received 200"). Echoes the host path, or empty if absent.
netlog_copy() {
    local out="${NET_TMP}/$1.log"
    docker cp "$1:${NETLOG}" "${out}" >/dev/null 2>&1 && echo "${out}"
}
netlog_lines() {
    local f; f="$(netlog_copy "$1")"
    if [ -n "${f}" ]; then wc -l < "${f}" | tr -d ' '; else echo 0; fi
}
a2_net_start=$(netlog_lines kraken-agent-two)

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

# Agent-to-agent P2P over the lazy stargz leg: of the pieces agent-two streamed,
# how many came peer->peer from agent-one (the overlayfs seeder) vs from origin.
# agent-one's peer id is the 'self' it stamps on its own events.
a1_log="$(netlog_copy kraken-agent-one)"
a2_log="$(netlog_copy kraken-agent-two)"
agent_one_id=$(head -n1 "${a1_log:-/dev/null}" 2>/dev/null \
    | python3 -c 'import sys,json;l=sys.stdin.read().strip();print(json.loads(l)["self"] if l else "")' \
    2>/dev/null || true)
if [ -n "${agent_one_id}" ] && [ -n "${a2_log}" ]; then
    p2p=$(tail -n +$(( a2_net_start + 1 )) "${a2_log}" 2>/dev/null \
        | python3 -c '
import sys, json
a1 = sys.argv[1]
from_a1 = from_origin = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    e = json.loads(line)
    if e.get("event") != "receive_piece":
        continue
    if e.get("peer") == a1:
        from_a1 += 1
    else:
        from_origin += 1
total = from_a1 + from_origin
pct = (100.0 * from_a1 / total) if total else 0.0
print(f"{from_a1} {from_origin} {total} {pct:.1f}")
' "${agent_one_id}" 2>/dev/null || true)
    read -r p_a1 p_or p_tot p_pct <<<"${p2p:-0 0 0 0.0}"
    echo >&2
    echo "============ agent-to-agent P2P over the lazy stargz leg ============" >&2
    echo "pieces agent-two streamed from agent-one (P2P): ${p_a1:-0}" >&2
    echo "pieces agent-two streamed from the origin:      ${p_or:-0}" >&2
    echo "peer-to-peer share of streamed pieces:          ${p_pct:-0.0}% (${p_tot:-0} total)" >&2
    echo "any P2P pieces > 0 proves agent<->agent works while lazily streaming" >&2
    echo "====================================================================" >&2
else
    echo "[estargz-bench] network_event log absent on agents; skipping P2P tally" >&2
    echo "[estargz-bench] enable network_event in config/agent/development.yaml" >&2
fi

if [ "${KEEP:-0}" = "1" ]; then
    log "KEEP=1: leaving ${CONTAINER} in place"
else
    docker rm -fv "${CONTAINER}" >/dev/null 2>&1 || true
fi

[ "${rc}" = "0" ] || { log "benchmark exited non-zero (${rc})"; exit "${rc}"; }
