#!/bin/bash
# nydus-snapshotter lazy-pull vs overlayfs full-pull over the SAME eStargz images,
# measured through the Kraken devcluster agent registries. Runs INSIDE the
# kraken-nydus container; drive it from the host with nydus_benchmark.sh, or
# directly:
#   docker exec kraken-nydus /usr/local/bin/run_e2e.sh [source-image] [cmd...]
#
# Flow: pull a source image, convert it to eStargz, push the converted image to
# Kraken (via the proxy), then pull/run it twice -- once with the overlayfs
# snapshotter (full pull, baseline) from agent :16000, and once with nydus (lazy,
# Range fetch via nydusd) from agent :17000 -- timing each.
#
# nydus mounts the SAME eStargz image lazily: its experimental enable_stargz mode
# downloads each layer's TOC, builds a per-layer bootstrap with nydus-image, and
# nydusd serves reads on demand from the agent over plain HTTP. There is no
# nydus-native RAFS conversion -- the image is identical to the one the estargz
# harness uses, so the nydus time-to-running number is directly comparable to the
# stargz number (same baseline, same cold agent :17000, same image).
#
# Why two agents + two namespaces: blobs are content-addressed, so a single agent
# would warm its cache on the first (overlayfs) leg and the nydus leg would no
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
AGENT_NYDUS="${KRAKEN_AGENT_NYDUS:-127.0.0.1:17000}"
REPO="${REPO:-nydus-e2e}"
TAG="run-$(date +%s)"

PUSH_NS=bench-push
OVERLAY_NS=bench-overlay
NYDUS_NS=bench-nydus

PROXY_REF="${PROXY}/${REPO}:${TAG}"
OVERLAY_REF="${AGENT_OVERLAY}/${REPO}:${TAG}"
NYDUS_REF="${AGENT_NYDUS}/${REPO}:${TAG}"

log()   { echo "[run_e2e] $*"; }
now()   { date +%s.%N; }
delta() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.2f", b - a }'; }

# reset_cold wipes the shared containerd content store and the nydus blob cache,
# then restarts containerd + the nydus snapshotter. containerd's content store is
# GLOBAL across namespaces, so the push leg's source pull would otherwise pre-warm
# every layer blob and the "cold" overlay/nydus pulls would never hit Kraken.
# Resetting here forces each leg to fetch from the agent registry for real. Only
# called when no nydus FUSE mounts are live (after push, after the overlay leg),
# so the rm never races a busy mount. nydusd is spawned by the snapshotter per
# mount, so leftover nydusd daemons are killed too.
reset_cold() {
    log "resetting containerd + nydus-snapshotter for a cold cache"
    pkill -x containerd 2>/dev/null || true
    pkill -f containerd-nydus-grpc 2>/dev/null || true
    pkill -x nydusd 2>/dev/null || true
    for _ in $(seq 1 20); do
        pgrep -x containerd >/dev/null 2>&1 || break
        sleep 0.2
    done
    rm -rf /var/lib/containerd/* /var/lib/nydus/* \
        /run/containerd-nydus/* 2>/dev/null || true
    mkdir -p /run/containerd-nydus /var/lib/nydus/cache
    containerd-nydus-grpc --config /etc/nydus/config.toml \
        --log-level info --log-to-stdout \
        >>/var/log/kraken-nydus/nydus-snapshotter.log 2>&1 &
    containerd >>/var/log/kraken-nydus/containerd.log 2>&1 &
    for _ in $(seq 1 60); do
        ctr version >/dev/null 2>&1 && break
        sleep 0.5
    done
    ctr version >/dev/null 2>&1 || { log "containerd not ready after reset"; exit 1; }
    ctr plugin ls | grep -qi nydus || log "WARNING: nydus plugin not registered"
}

# --- Publish eStargz-converted image to Kraken ------------------------------

log "pulling source image ${SRC_IMAGE}"
nerdctl --namespace "${PUSH_NS}" pull "${SRC_IMAGE}"

# Convert to eStargz directly into the Kraken-tagged ref. The converted image is
# a valid OCI image, so overlayfs can still pull it in full for the baseline leg,
# and nydus can lazily mount the very same blobs -- apples-to-apples.
log "converting ${SRC_IMAGE} to eStargz -> ${PROXY_REF}"
nerdctl --namespace "${PUSH_NS}" image convert --oci --estargz \
    "${SRC_IMAGE}" "${PROXY_REF}"

log "pushing eStargz image to Kraken proxy ${PROXY_REF}"
nerdctl --namespace "${PUSH_NS}" push "${PROXY_REF}"

# Both legs run a single `nerdctl run`, which auto-pulls the image as part of
# starting the container. This is the true time-to-running metric: for overlayfs
# the run blocks on the full layer download; for nydus the container starts once
# the TOCs land and bootstraps are built, then layer bytes stream in lazily on
# first read. A separate `pull` step is deliberately avoided -- it would hide the
# lazy win, since the point is that the container starts before layers finish.

# --- Baseline: overlayfs full pull from cold agent :16000 -------------------

reset_cold
log "BASELINE overlayfs: run (auto-pulls ${OVERLAY_REF})"
b_run_start=$(now)
nerdctl --namespace "${OVERLAY_NS}" --snapshotter overlayfs run \
    --rm --net none --pull always "${OVERLAY_REF}" "${RUN_CMD[@]}"
b_run_end=$(now)

# --- Streaming: nydus lazy pull from cold agent :17000 ----------------------
#
# eStargz discovery is implicit: the TOC is embedded in each layer blob. nydus
# (enable_stargz) reads each TOC, builds a bootstrap, and nydusd Range-reads layer
# bytes on demand from the agent over plain HTTP (scheme=http in the nydusd
# registry backend config).

reset_cold
log "NYDUS lazy: run (auto-pulls ${NYDUS_REF})"
s_run_start=$(now)
nerdctl --namespace "${NYDUS_NS}" --snapshotter nydus run \
    --rm --net none --pull always "${NYDUS_REF}" "${RUN_CMD[@]}"
s_run_end=$(now)

# --- Results ----------------------------------------------------------------

echo
echo "================== nydus e2e: overlayfs vs nydus ===================="
printf "image:     %s\n" "${SRC_IMAGE}"
printf "%-12s %18s\n" "snapshotter" "time-to-running(s)"
printf "%-12s %18s\n" "overlayfs" "$(delta "${b_run_start}" "${b_run_end}")"
printf "%-12s %18s\n" "nydus"     "$(delta "${s_run_start}" "${s_run_end}")"
echo "===================================================================="
echo "overlayfs       = full layer pull + start from cold agent ${AGENT_OVERLAY}"
echo "nydus           = lazy Range pull + start from cold agent ${AGENT_NYDUS}"
echo "time-to-running = single 'nerdctl run' wall clock; each leg cold"
echo "all times in seconds"

# --- Lazy-pull verification -------------------------------------------------
# nydus has no single "remote-snapshot-prepared:true" line like stargz. The
# decisive proof is the host-side 206-vs-200 byte split (printed by
# nydus_benchmark.sh from the agent access logs): a lazy nydus run fetches far
# fewer bytes via ranged 206 GETs than the full overlayfs 200 pull. Here we add a
# best-effort in-container signal: the snapshotter logs the eStargz path (stargz
# ref / TOC handling) at info level, and any mount error means a layer did not go
# lazy. A non-zero "stargz path" count with zero errors corroborates the bytes.
nydus_log=/var/log/kraken-nydus/nydus-snapshotter.log
stargz_hits=$(grep -ci 'stargz' "${nydus_log}" 2>/dev/null || true)
mount_errs=$(grep -ciE 'failed to mount|prepare.*fail|error.*nydusd' "${nydus_log}" 2>/dev/null || true)
echo
echo "nydus eStargz path log hits (stargz ref/TOC handling): ${stargz_hits:-0}"
echo "snapshotter mount errors (should be 0 for a lazy run): ${mount_errs:-0}"
echo "decisive lazy proof is the 206-vs-200 byte split from nydus_benchmark.sh"
