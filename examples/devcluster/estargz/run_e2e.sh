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
# Before the stargz leg the origin's blob cache is evicted (forcecleanup) so that
# leg is served by a COLD origin: instead of seeding a pre-warmed whole blob, the
# origin fetches a tiny metainfo sidecar (.kmeta) and lazily Range-fetches only
# the pieces the agent requests from the backend (testfs). The host script
# (estargz_benchmark.sh) then reports origin->backend egress -- it should be far
# below the full image size, the cold-origin counterpart of the agent-side proof.
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
AGENT_OVERLAY_SERVER="${KRAKEN_AGENT_OVERLAY_SERVER:-127.0.0.1:16002}"
AGENT_STARGZ_SERVER="${KRAKEN_AGENT_STARGZ_SERVER:-127.0.0.1:17002}"
ORIGIN="${KRAKEN_ORIGIN:-127.0.0.1:15002}"
# COLD_AGENT=1 colds agent-one (the overlay seeder) before the stargz leg, so
# that leg can no longer stream pieces peer->peer from it and must source them
# from the cold origin -- exercising the Stack B lazy range-fetch end to end.
COLD_AGENT="${COLD_AGENT:-0}"
# COLD_OVERLAY=1 makes the overlay (full-pull) leg itself source from a COLD
# origin: the origin and both agents are colded before it, so the full image is
# range-fetched from the backend rather than served warm from an agent cache.
COLD_OVERLAY="${COLD_OVERLAY:-0}"
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

# json_array encodes its arguments as a JSON string array, e.g.
# json_array python3 -c x -> ["python3","-c","x"]. ctr-remote optimize takes its
# --entrypoint/--args as JSON arrays and the harness has no jq, so RUN_CMD is
# encoded here. Escapes backslash and double-quote; CLI args carry no control
# chars, so this is sufficient.
json_array() {
    local out="" a sep=""
    for a in "$@"; do
        a=${a//\\/\\\\}
        a=${a//\"/\\\"}
        out="${out}${sep}\"${a}\""
        sep=,
    done
    printf '[%s]' "${out}"
}

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

# cold_origin evicts the origin's local blob cache so the stargz leg is served by
# a COLD origin. forcecleanup runs writeback FIRST (so the blob + .kmeta metainfo
# sidecar are guaranteed on the backend) and only then deletes the warm cache
# files. partial=true also evicts the cold-blob download store (download/ +
# cache-partial/), where a prior leg's lazy range-fetches land -- without it that
# partial store survives and the next leg is served warm from it, not cold. A
# cold origin can no longer seed a pre-warmed whole blob: it fetches the tiny
# sidecar, then Range-fetches + CRC-verifies only the pieces the agent asks for.
# Called after the warm overlay leg and before the stargz leg, so the
# origin->backend egress measured over the stargz leg is purely lazy fetches.
# Runs in-container over the host network (curl), so there is no docker exec and
# no pkill self-match like reset_cold has to guard against.
cold_origin() {
    log "cold origin: POST http://${ORIGIN}/forcecleanup?ttl_hr=0&partial=true"
    code=$(curl -sS -o /dev/null -w '%{http_code}' \
        -X POST "http://${ORIGIN}/forcecleanup?ttl_hr=0&partial=true" || echo 000)
    if [ "${code}" != "200" ]; then
        log "WARNING: forcecleanup returned ${code} (origin may not be cold)"
    fi
}

# fetch_manifest <registry> <reference> -> manifest/index JSON on stdout (empty
# on error). The Accept header covers OCI and Docker manifest + index types.
fetch_manifest() {
    curl -fsSL \
        -H "Accept: application/vnd.oci.image.index.v1+json" \
        -H "Accept: application/vnd.oci.image.manifest.v1+json" \
        -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
        -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
        "http://${1}/v2/${REPO}/manifests/${2}" 2>/dev/null || true
}

# image_blob_digests <registry> -> unique sha256 digests for REPO:TAG (config +
# layers), descending one level when the tag resolves to an OCI/Docker index.
# The harness has no jq, so digests are scraped with grep; fetching a layer or
# config digest as a manifest just 404s and contributes nothing, and DELETE on a
# digest the agent never cached is a harmless no-op, so over-collection is safe.
image_blob_digests() {
    local reg="$1" root d
    root="$(fetch_manifest "${reg}" "${TAG}")"
    {
        echo "${root}" | grep -oE 'sha256:[0-9a-f]{64}' || true
        echo "${root}" | grep -oE 'sha256:[0-9a-f]{64}' | while read -r d; do
            fetch_manifest "${reg}" "${d}" | grep -oE 'sha256:[0-9a-f]{64}' || true
        done
    } | sort -u
}

# cold_agent removes the converted image's blobs from an agent via the agent
# DELETE API, so a subsequent leg can no longer stream those pieces peer->peer
# from it. DELETE /blobs/{digest} (no namespace prefix) hits the agent SERVER
# port and drops the in-memory torrent (stops announcing) and the cached bytes,
# fully colding the peer for that blob. With the origin also cold, the puller
# then sources pieces from the origin's lazy range-fetch. Args:
# <label> <registry> <server>.
cold_agent() {
    local label="$1" reg="$2" server="$3" d code n=0
    log "cold ${label}: DELETE converted image blobs on ${server}"
    for d in $(image_blob_digests "${reg}"); do
        code=$(curl -sS -o /dev/null -w '%{http_code}' \
            -X DELETE "http://${server}/blobs/${d}" || echo 000)
        log "  DELETE ${d} -> ${code}"
        n=$((n + 1))
    done
    [ "${n}" -gt 0 ] || log "WARNING: no blob digests found; ${label} not colded"
}

# --- Publish eStargz-converted image to Kraken ------------------------------

log "pulling source image ${SRC_IMAGE}"
nerdctl --namespace "${PUSH_NS}" pull "${SRC_IMAGE}"

# Convert to eStargz WITH a prefetch profile, so the snapshotter bulk-prefetches
# the workload's working set on mount instead of paying per-file lazy-fetch
# latency. ctr-remote optimize runs RUN_CMD against the source, records the files
# it touches (--period bounds the monitor window), reorders them to the front of
# each layer and stamps a .prefetch.landmark -- producing the estargz image
# directly as the Kraken-tagged ref. (nerdctl's --estargz-record-in needs a
# record file that only `ctr-remote optimize --record-out` can produce, so we
# optimize directly here.) Recording RUN_CMD keeps the prefetched set == what the
# legs actually run. ctr-remote shares containerd's content store with nerdctl in
# the same namespace, so the push below uploads the optimized image. The result
# stays a valid OCI image, so the overlayfs baseline still pulls it in full.
log "converting+optimizing ${SRC_IMAGE} to eStargz -> ${PROXY_REF}"
ctr-remote --namespace "${PUSH_NS}" image optimize --oci \
    --period="${OPTIMIZE_PERIOD:-30}" \
    --entrypoint="$(json_array "${RUN_CMD[0]}")" \
    --args="$(json_array "${RUN_CMD[@]:1}")" \
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
#
# With COLD_OVERLAY=1 the origin and both agents are colded first, so this
# full-pull leg sources the whole image from the origin's lazy range-fetch
# (~100% backend egress) -- the full-pull counterpart of the stargz leg's
# lazy cold-origin egress. Default (warm) leaves agent-one seeding from cache.
if [ "${COLD_OVERLAY}" = "1" ]; then
    cold_origin
    cold_agent agent-one "${AGENT_OVERLAY}" "${AGENT_OVERLAY_SERVER}"
    cold_agent agent-two "${AGENT_STARGZ}" "${AGENT_STARGZ_SERVER}"
fi
reset_cold
log "BASELINE overlayfs: run (auto-pulls ${OVERLAY_REF})"
b_run_start=$(now)
nerdctl --namespace "${OVERLAY_NS}" --snapshotter overlayfs run \
    --rm --net none --pull always "${OVERLAY_REF}" "${RUN_CMD[@]}"
b_run_end=$(now)
# Leg window for the host's per-leg origin->backend egress split: only testfs
# download lines timestamped within [start,end] are attributed to this leg.
log "MARK overlay_window ${b_run_start} ${b_run_end}"

# --- Streaming: stargz lazy pull from cold agent :17000 ---------------------
#
# eStargz discovery is implicit: the TOC is embedded in each layer blob, so the
# snapshotter resolves laziness from the image itself -- no index artifact, no
# Referrers API, no derived tag. Layer Range reads go to the agent over plain
# HTTP (insecure mirror in stargz_config.toml).
#
# The origin is colded here so the stargz leg drives the origin's lazy Range-
# fetch from the backend. With COLD_AGENT=1 agent-one is colded too, forcing the
# pieces through the origin instead of peer->peer from the warm overlay seeder.
# (COLD_OVERLAY=1 makes the overlay leg cold too; the host attributes each leg's
# backend egress by its own run window, so the two never conflate.)

cold_origin
if [ "${COLD_AGENT}" = "1" ]; then
    cold_agent agent-one "${AGENT_OVERLAY}" "${AGENT_OVERLAY_SERVER}"
fi
reset_cold
log "STARGZ lazy: run (auto-pulls ${STARGZ_REF})"
s_run_start=$(now)
nerdctl --namespace "${STARGZ_NS}" --snapshotter stargz run \
    --rm --net none --pull always "${STARGZ_REF}" "${RUN_CMD[@]}"
s_run_end=$(now)
log "MARK stargz_window ${s_run_start} ${s_run_end}"

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
# count means a layer fell back to a full pull. The host script
# estargz_benchmark.sh prints the per-byte proofs that live outside this
# container: the agent-side 206-vs-full-pull, and the cold origin's
# origin->backend (testfs) egress -- which should be far below the full image.
stargz_log=/var/log/kraken-estargz/stargz-snapshotter.log
lazy_true=$(grep -c '"remote-snapshot-prepared":"true"' "${stargz_log}" 2>/dev/null || true)
lazy_fail=$(grep -c '"remote-snapshot-prepared":"false"' "${stargz_log}" 2>/dev/null || true)
echo
echo "lazy layers remote-mounted (remote-snapshot-prepared:true): ${lazy_true:-0}"
echo "failed remote-snapshot prepares (must be 0 for a lazy run): ${lazy_fail:-0}"
