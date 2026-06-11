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

PROXY="${KRAKEN_PROXY:-127.0.0.1:15000}"
AGENT_OVERLAY="${KRAKEN_AGENT_OVERLAY:-127.0.0.1:16000}"
AGENT_SOCI="${KRAKEN_AGENT_SOCI:-127.0.0.1:17000}"
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

# reset_cold wipes the shared containerd content store and the soci blob cache,
# then restarts both daemons. containerd's content store is GLOBAL across
# namespaces, so the push leg's source pull would otherwise pre-warm every layer
# blob and the "cold" overlay/soci pulls would never hit Kraken. Resetting here
# forces each leg to fetch from the agent registry for real. Only called when no
# soci FUSE mounts are live (after push, after the overlay leg), so the rm never
# races a busy mount.
reset_cold() {
    log "resetting containerd + soci-snapshotter for a cold cache"
    pkill -x containerd 2>/dev/null || true
    pkill -f soci-snapshotter-grpc 2>/dev/null || true
    for _ in $(seq 1 20); do
        pgrep -x containerd >/dev/null 2>&1 || break
        sleep 0.2
    done
    rm -rf /var/lib/containerd/* /var/lib/soci-snapshotter-grpc/* \
        /run/soci-snapshotter-grpc/* 2>/dev/null || true
    mkdir -p /run/soci-snapshotter-grpc
    soci-snapshotter-grpc --config /etc/soci-snapshotter-grpc/config.toml \
        >>/var/log/kraken-soci/soci-snapshotter.log 2>&1 &
    containerd >>/var/log/kraken-soci/containerd.log 2>&1 &
    for _ in $(seq 1 60); do
        ctr version >/dev/null 2>&1 && break
        sleep 0.5
    done
    ctr version >/dev/null 2>&1 || { log "containerd not ready after reset"; exit 1; }
    ctr plugin ls | grep -qi soci || log "WARNING: soci plugin not registered"
}

# --- Publish image + SOCI index to Kraken -----------------------------------

log "pulling source image ${SRC_IMAGE}"
nerdctl --namespace "${PUSH_NS}" pull "${SRC_IMAGE}"
nerdctl --namespace "${PUSH_NS}" tag "${SRC_IMAGE}" "${PROXY_REF}"

log "pushing image to Kraken proxy ${PROXY_REF}"
nerdctl --namespace "${PUSH_NS}" push "${PROXY_REF}"

log "creating + pushing SOCI index (fallback mode)"
soci --namespace "${PUSH_NS}" create "${PROXY_REF}"
soci --namespace "${PUSH_NS}" push --existing-index allow --plain-http "${PROXY_REF}"

# Capture the SOCI index digest now (before reset_cold wipes the local soci
# store). Kraken doesn't implement the OCI Referrers API, so soci's automatic
# discovery fails and falls back to a full pull; passing the digest explicitly
# at pull time makes the snapshotter fetch the index directly and go lazy.
SOCI_INDEX_DIGEST="$(soci --namespace "${PUSH_NS}" index list -q --ref "${PROXY_REF}" | head -1)"
log "soci index digest: ${SOCI_INDEX_DIGEST:-<none>}"

# Both legs run a single `nerdctl run`, which auto-pulls the image as part of
# starting the container. This is the true time-to-running metric: for overlayfs
# the run blocks on the full layer download; for soci the container starts as
# soon as the index + ztoc land and layer bytes stream in lazily on first read.
# A separate `pull` step is deliberately avoided -- it would hide soci's win,
# since the point is that the container starts before the layers finish pulling.

# --- Baseline: overlayfs full pull from cold agent :16000 -------------------

reset_cold
log "BASELINE overlayfs: run (auto-pulls ${OVERLAY_REF})"
b_run_start=$(now)
nerdctl --namespace "${OVERLAY_NS}" --snapshotter overlayfs run \
    --rm --net none --pull always "${OVERLAY_REF}" "${RUN_CMD[@]}"
b_run_end=$(now)

# --- Streaming: soci lazy pull from cold agent :17000 -----------------------
#
# No --soci-index-digest here: `nerdctl run` doesn't accept it. The snapshotter
# discovers the index automatically -- the Referrers API 404s on Kraken, so
# oras-go falls back to the sha256-<manifest> tag that `soci push` wrote, fetched
# over the insecure HTTP mirror configured in soci_config.toml.

reset_cold
log "SOCI lazy: run (auto-pulls ${SOCI_REF}, index ${SOCI_INDEX_DIGEST:-<auto>})"
s_run_start=$(now)
nerdctl --namespace "${SOCI_NS}" --snapshotter soci run \
    --rm --net none --pull always "${SOCI_REF}" "${RUN_CMD[@]}"
s_run_end=$(now)

# --- Results ----------------------------------------------------------------

echo
echo "==================== soci e2e: overlayfs vs soci ===================="
printf "image:     %s\n" "${SRC_IMAGE}"
printf "%-12s %18s\n" "snapshotter" "time-to-running(s)"
printf "%-12s %18s\n" "overlayfs" "$(delta "${b_run_start}" "${b_run_end}")"
printf "%-12s %18s\n" "soci"      "$(delta "${s_run_start}" "${s_run_end}")"
echo "===================================================================="
echo "overlayfs       = full layer pull + start from cold agent ${AGENT_OVERLAY}"
echo "soci            = lazy Range pull + start from cold agent ${AGENT_SOCI}"
echo "time-to-running = single 'nerdctl run' wall clock; each leg cold"
echo "all times in seconds"

# --- Lazy-pull verification -------------------------------------------------
# Confirm soci actually remote-mounted layers (lazy) instead of pulling them in
# full. Every layer that goes lazy logs remote-snapshot-prepared:true; layers
# without a ztoc (below soci create's min size) are pulled in full and never log
# a successful prepare. https_errs must be 0 -- a non-zero count means the
# artifact fetch fell back to HTTPS and the leg was NOT actually lazy. The
# per-byte 206-vs-full-pull proof is printed by the host script soci_benchmark.sh
# (the agent access logs live outside this container).
soci_log=/var/log/kraken-soci/soci-snapshotter.log
lazy_true=$(grep -c '"remote-snapshot-prepared":"true"' "${soci_log}" 2>/dev/null || true)
https_errs=$(grep -c "HTTP response to HTTPS" "${soci_log}" 2>/dev/null || true)
echo
echo "lazy layers remote-mounted (remote-snapshot-prepared:true): ${lazy_true:-0}"
echo "soci HTTPS-to-HTTP fallback errors (must be 0 for a lazy run): ${https_errs:-0}"
