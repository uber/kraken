#!/bin/bash
# soci-snapshotter lazy-pull vs overlayfs full-pull, measured through the Kraken
# devcluster agent registries. Runs INSIDE the kraken-soci container; drive it
# from the host with soci_benchmark.sh, or directly:
#   docker exec kraken-soci /usr/local/bin/run_e2e.sh [source-image] [cmd...]
#
# Flow: pull a source image, push it + a SOCI index to Kraken (via the proxy),
# then pull/run it twice -- once with the overlayfs snapshotter (full pull,
# baseline) from agent :16000, and once with soci (lazy, Range fetch) from agent
# :17000 -- timing each.
#
# Why two agents + two namespaces: blobs are content-addressed, so a single agent
# would warm its cache on the first (overlayfs) leg and the soci leg would no
# longer be cold. Each leg therefore targets a different cold agent and runs in
# its own containerd namespace so the local content store is not shared either.

set -eu

SRC_IMAGE="${1:-public.ecr.aws/docker/library/python:3.12}"
RUN_CMD=("${@:2}")
if [ "${#RUN_CMD[@]}" -eq 0 ]; then
    RUN_CMD=(python3 -c "print('ready')")
fi

PROXY="${KRAKEN_PROXY:-host.docker.internal:15000}"
AGENT_OVERLAY="${KRAKEN_AGENT_OVERLAY:-host.docker.internal:16000}"
AGENT_SOCI="${KRAKEN_AGENT_SOCI:-host.docker.internal:17000}"
REPO="${REPO:-soci-e2e}"
TAG="run-$(date +%s)"

PUSH_NS=bench-push
OVERLAY_NS=bench-overlay
SOCI_NS=bench-soci

PROXY_REF="${PROXY}/${REPO}:${TAG}"
OVERLAY_REF="${AGENT_OVERLAY}/${REPO}:${TAG}"
SOCI_REF="${AGENT_SOCI}/${REPO}:${TAG}"

log()   { echo "[run_e2e] $*"; }
now()   { date +%s.%N; }
delta() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.2f", b - a }'; }

# --- Publish image + SOCI index to Kraken -----------------------------------

log "pulling source image ${SRC_IMAGE}"
nerdctl --namespace "${PUSH_NS}" pull "${SRC_IMAGE}"
nerdctl --namespace "${PUSH_NS}" tag "${SRC_IMAGE}" "${PROXY_REF}"

log "pushing image to Kraken proxy ${PROXY_REF}"
nerdctl --namespace "${PUSH_NS}" push "${PROXY_REF}"

log "creating + pushing SOCI index (fallback mode)"
soci --namespace "${PUSH_NS}" create "${PROXY_REF}"
soci --namespace "${PUSH_NS}" push --existing-index allow "${PROXY_REF}"

# --- Baseline: overlayfs full pull from cold agent :16000 -------------------

log "BASELINE overlayfs: pull ${OVERLAY_REF}"
b_pull_start=$(now)
nerdctl --namespace "${OVERLAY_NS}" --snapshotter overlayfs pull "${OVERLAY_REF}"
b_pull_end=$(now)

log "BASELINE overlayfs: run"
b_run_start=$(now)
nerdctl --namespace "${OVERLAY_NS}" --snapshotter overlayfs run \
    --rm --net none "${OVERLAY_REF}" "${RUN_CMD[@]}"
b_run_end=$(now)

# --- Streaming: soci lazy pull from cold agent :17000 -----------------------

log "SOCI lazy: pull ${SOCI_REF}"
s_pull_start=$(now)
nerdctl --namespace "${SOCI_NS}" --snapshotter soci pull "${SOCI_REF}"
s_pull_end=$(now)

log "SOCI lazy: run"
s_run_start=$(now)
nerdctl --namespace "${SOCI_NS}" --snapshotter soci run \
    --rm --net none "${SOCI_REF}" "${RUN_CMD[@]}"
s_run_end=$(now)

# --- Results ----------------------------------------------------------------

echo
echo "==================== soci e2e: overlayfs vs soci ===================="
printf "image:     %s\n" "${SRC_IMAGE}"
printf "%-12s %12s %12s\n" "snapshotter" "pull(s)" "run(s)"
printf "%-12s %12s %12s\n" "overlayfs" \
    "$(delta "${b_pull_start}" "${b_pull_end}")" \
    "$(delta "${b_run_start}" "${b_run_end}")"
printf "%-12s %12s %12s\n" "soci" \
    "$(delta "${s_pull_start}" "${s_pull_end}")" \
    "$(delta "${s_run_start}" "${s_run_end}")"
echo "===================================================================="
echo "overlayfs = full layer pull from cold agent ${AGENT_OVERLAY}"
echo "soci      = lazy Range pull from cold agent ${AGENT_SOCI}"
echo "all times in seconds"
