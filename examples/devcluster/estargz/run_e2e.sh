#!/bin/bash
# stargz-snapshotter (eStargz) lazy-pull vs overlayfs full-pull, measured through
# the Kraken devcluster agent registries. Runs INSIDE the kraken-estargz
# container; drive it from the host with estargz_benchmark.sh, or directly:
#   docker exec kraken-estargz /usr/local/bin/run_e2e.sh [source-image] [cmd...]
#
# Flow: pull a source image, convert it to eStargz, push the converted image to
# Kraken (via the proxy), then pull/run it twice -- once with the overlayfs
# snapshotter (full pull, baseline) from agent :16000, and once with stargz
# (lazy, Range fetch) from agent :17000 -- timing each.
#
# Unlike soci, eStargz has no separate index artifact: the TOC is embedded in
# each converted layer, so there is no `create`/`push --index` step and no
# Referrers/derived-tag discovery -- conversion is the one extra step instead.
# The converted image stays backward-compatible, so the overlayfs baseline pulls
# the very same image in full (apples-to-apples lazy-vs-full of identical bytes).
#
# Why two agents + two namespaces: blobs are content-addressed, so a single agent
# would warm its cache on the first (overlayfs) leg and the stargz leg would no
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
AGENT_STARGZ="${KRAKEN_AGENT_STARGZ:-127.0.0.1:17000}"
REPO="${REPO:-estargz-e2e}"
TAG="run-$(date +%s)"

PUSH_NS=bench-push
OVERLAY_NS=bench-overlay
STARGZ_NS=bench-stargz

PROXY_REF="${PROXY}/${REPO}:${TAG}"
OVERLAY_REF="${AGENT_OVERLAY}/${REPO}:${TAG}"
STARGZ_REF="${AGENT_STARGZ}/${REPO}:${TAG}"

log()   { echo "[run_e2e] $*"; }
now()   { date +%s.%N; }
delta() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.2f", b - a }'; }

# reset_cold wipes the shared containerd content store and the stargz blob cache,
# then restarts both daemons. containerd's content store is GLOBAL across
# namespaces, so the push leg's source pull would otherwise pre-warm every layer
# blob and the "cold" overlay/stargz pulls would never hit Kraken. Resetting here
# forces each leg to fetch from the agent registry for real. Only called when no
# stargz FUSE mounts are live (after push, after the overlay leg), so the rm never
# races a busy mount.
reset_cold() {
    log "resetting containerd + containerd-stargz-grpc for a cold cache"
    pkill -x containerd 2>/dev/null || true
    pkill -f containerd-stargz-grpc 2>/dev/null || true
    for _ in $(seq 1 20); do
        pgrep -x containerd >/dev/null 2>&1 || break
        sleep 0.2
    done
    rm -rf /var/lib/containerd/* /var/lib/containerd-stargz-grpc/* \
        /run/containerd-stargz-grpc/* 2>/dev/null || true
    mkdir -p /run/containerd-stargz-grpc
    containerd-stargz-grpc --log-level=debug \
        --config /etc/containerd-stargz-grpc/config.toml \
        >>/var/log/kraken-estargz/stargz-snapshotter.log 2>&1 &
    containerd >>/var/log/kraken-estargz/containerd.log 2>&1 &
    for _ in $(seq 1 60); do
        ctr version >/dev/null 2>&1 && break
        sleep 0.5
    done
    ctr version >/dev/null 2>&1 || { log "containerd not ready after reset"; exit 1; }
    ctr plugin ls | grep -qi stargz || log "WARNING: stargz plugin not registered"
}

# --- Publish eStargz-converted image to Kraken ------------------------------

log "pulling source image ${SRC_IMAGE}"
nerdctl --namespace "${PUSH_NS}" pull "${SRC_IMAGE}"

# Convert to eStargz directly into the Kraken-tagged ref. --estargz-min-chunk-size=0
# chunks at file boundaries for the finest-grained lazy fetch (only touched files
# are pulled). The converted image is a valid OCI image, so overlayfs can still
# pull it in full for the baseline leg.
log "converting ${SRC_IMAGE} to eStargz -> ${PROXY_REF}"
nerdctl --namespace "${PUSH_NS}" image convert --oci --estargz \
    "${SRC_IMAGE}" "${PROXY_REF}"

log "pushing eStargz image to Kraken proxy ${PROXY_REF}"
nerdctl --namespace "${PUSH_NS}" push "${PROXY_REF}"

# Both legs run a single `nerdctl run`, which auto-pulls the image as part of
# starting the container. This is the true time-to-running metric: for overlayfs
# the run blocks on the full layer download; for stargz the container starts as
# soon as the TOC lands and layer bytes stream in lazily on first read. A
# separate `pull` step is deliberately avoided -- it would hide stargz's win,
# since the point is that the container starts before the layers finish pulling.

# --- Baseline: overlayfs full pull from cold agent :16000 -------------------

reset_cold
log "BASELINE overlayfs: run (auto-pulls ${OVERLAY_REF})"
b_run_start=$(now)
nerdctl --namespace "${OVERLAY_NS}" --snapshotter overlayfs run \
    --rm --net none --pull always "${OVERLAY_REF}" "${RUN_CMD[@]}"
b_run_end=$(now)

# --- Streaming: stargz lazy pull from cold agent :17000 ---------------------
#
# eStargz discovery is implicit: the TOC is embedded in each layer blob, so the
# snapshotter resolves laziness from the image itself -- no index artifact, no
# Referrers API, no derived tag. Layer Range reads go to the agent over plain
# HTTP (insecure mirror in stargz_config.toml).

reset_cold
log "STARGZ lazy: run (auto-pulls ${STARGZ_REF})"
s_run_start=$(now)
nerdctl --namespace "${STARGZ_NS}" --snapshotter stargz run \
    --rm --net none --pull always "${STARGZ_REF}" "${RUN_CMD[@]}"
s_run_end=$(now)

# --- Results ----------------------------------------------------------------

echo
echo "================== estargz e2e: overlayfs vs stargz =================="
printf "image:     %s\n" "${SRC_IMAGE}"
printf "%-12s %18s\n" "snapshotter" "time-to-running(s)"
printf "%-12s %18s\n" "overlayfs" "$(delta "${b_run_start}" "${b_run_end}")"
printf "%-12s %18s\n" "stargz"    "$(delta "${s_run_start}" "${s_run_end}")"
echo "===================================================================="
echo "overlayfs       = full layer pull + start from cold agent ${AGENT_OVERLAY}"
echo "stargz          = lazy Range pull + start from cold agent ${AGENT_STARGZ}"
echo "time-to-running = single 'nerdctl run' wall clock; each leg cold"
echo "all times in seconds"

# --- Lazy-pull verification -------------------------------------------------
# Confirm stargz actually remote-mounted layers (lazy) instead of pulling them in
# full. Every layer that goes lazy logs remote-snapshot-prepared:true (DEBUG
# level -- the daemon runs with -log-level=debug). A non-zero "false"/failed
# count means a layer fell back to a full pull. The per-byte 206-vs-full-pull
# proof is printed by the host script estargz_benchmark.sh (the agent access logs
# live outside this container).
stargz_log=/var/log/kraken-estargz/stargz-snapshotter.log
lazy_true=$(grep -c '"remote-snapshot-prepared":"true"' "${stargz_log}" 2>/dev/null || true)
lazy_fail=$(grep -c '"remote-snapshot-prepared":"false"' "${stargz_log}" 2>/dev/null || true)
echo
echo "lazy layers remote-mounted (remote-snapshot-prepared:true): ${lazy_true:-0}"
echo "failed remote-snapshot prepares (must be 0 for a lazy run): ${lazy_fail:-0}"
