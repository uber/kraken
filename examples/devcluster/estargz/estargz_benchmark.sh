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
#   REBUILD=1    force a rebuild of the kraken-estargz:dev harness image
#   KEEP=1       leave the kraken-estargz container running after the benchmark
#   COLD_AGENT=1 cold agent-one before the stargz leg so its pieces are sourced
#                from the cold origin (lazy range-fetch) instead of peer->peer
#   COLD_OVERLAY=1 cold the origin + both agents before the OVERLAY leg too, so
#                that full-pull leg's backend egress is measured from a cold
#                origin (~100% of the image) alongside stargz's lazy fraction

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

if [ "${COLD_AGENT:-0}" = "1" ]; then
    log "COLD_AGENT=1: agent-one is colded before the stargz leg"
    log "  expect P2P-from-agent-one ~0 and cold origin->testfs 206 fetches > 0"
else
    log "COLD_AGENT=0: stargz leg may stream peer->peer from the warm agent-one"
    log "  set COLD_AGENT=1 to force pieces through the cold origin"
fi

if [ "${COLD_OVERLAY:-0}" = "1" ]; then
    log "COLD_OVERLAY=1: overlay leg is served by a cold origin (both agents cold)"
    log "  expect overlay origin->testfs egress ~100% of the image (full pull)"
else
    log "COLD_OVERLAY=0: overlay leg serves warm from agent-one (origin untouched)"
    log "  set COLD_OVERLAY=1 to measure the full-pull cold-origin egress"
fi

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

# blob_bytes <container> <start-line> <status> -> total bytes (numeric only).
# Used for the cold-origin egress denominator: the overlayfs leg pulls the SAME
# converted image in full, so agent-one's 200 bytes are the full image size.
blob_bytes() {
    docker logs "$1" 2>/dev/null | tail -n +"$(( $2 + 1 ))" \
        | awk -v st=" $3 " '$0 ~ "/blobs/" && index($0, st) {b+=$(NF-2)} END {print b+0}'
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

# Snapshot the testfs (backend) download log too, for the cold-origin egress
# tally. run_e2e colds the origin before the stargz leg, so that leg's pieces are
# served by the origin Range-fetching the backend: the origin fetches the tiny
# .kmeta metainfo sidecar (200) then one ranged GET (206) per touched piece. The
# testfs server logs each as a "testfs download" line with name/status/bytes.
# Push uploads are POSTs and the warm overlay leg is served from the origin
# cache, so NO "testfs download" line over this run belongs to anything but the
# cold stargz leg -- snapshot the count now and attribute only the new lines.
# The log lives in the herd container; copy it out with docker cp (no exec).
TESTFS_LOG=/var/log/kraken/kraken-testfs/stdout.log
testfs_log_copy() {
    local out="${NET_TMP}/testfs.log"
    docker cp "kraken-herd:${TESTFS_LOG}" "${out}" >/dev/null 2>&1 && echo "${out}"
}
testfs_log_lines() {
    local f; f="$(testfs_log_copy)"
    if [ -n "${f}" ]; then wc -l < "${f}" | tr -d ' '; else echo 0; fi
}
testfs_start=$(testfs_log_lines)

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

# Re-read the (now complete) logs to recover the per-leg run windows run_e2e
# stamped with `MARK <leg>_window <start> <end>` (epoch seconds). The host buckets
# each testfs download line into the leg whose window contains its timestamp, so
# the overlay (full-pull) and stargz (lazy) cold-origin egress never conflate.
container_logs=$(docker logs "${CONTAINER}" 2>&1 || true)
read -r ov_start ov_end <<<"$(printf '%s\n' "${container_logs}" \
    | sed -n 's/.*MARK overlay_window \([0-9.][0-9.]*\) \([0-9.][0-9.]*\).*/\1 \2/p' \
    | tail -n1)" || true
read -r sg_start sg_end <<<"$(printf '%s\n' "${container_logs}" \
    | sed -n 's/.*MARK stargz_window \([0-9.][0-9.]*\) \([0-9.][0-9.]*\).*/\1 \2/p' \
    | tail -n1)" || true

# Bytes fetched from the Kraken agents during this run (host-side proof).
echo
echo "==================== bytes fetched from Kraken agents ===============" >&2
echo "stargz    (lazy, agent-two 206): $(blob_stats kraken-agent-two "${a2_start}" 206)" >&2
echo "overlayfs (full, agent-one 200): $(blob_stats kraken-agent-one "${a1_start}" 200)" >&2
echo "stargz should be FAR below overlayfs; if not, the layer was read in full" >&2
echo "====================================================================" >&2

# Cold-origin egress, split per leg by run window: the bytes the origin Range-
# fetched from the backend (testfs) during each leg. Each testfs download line is
# attributed to the leg whose MARK window contains its timestamp, so the overlay
# (full pull) and stargz (lazy) cold-origin egress never conflate -- even with
# COLD_OVERLAY=1, when both legs go through the cold origin. agent-one's 200-byte
# total is the full converted image, the denominator for each leg's share.
overlay_full_bytes=$(blob_bytes kraken-agent-one "${a1_start}" 200)
testfs_log="$(testfs_log_copy)"
echo >&2
echo "===== cold origin -> backend (testfs) egress, per leg (by run window) =====" >&2
if [ -z "${testfs_log}" ]; then
    echo "[estargz-bench] testfs log absent on kraken-herd; skipping origin tally" >&2
elif [ -z "${ov_start:-}" ] || [ -z "${sg_start:-}" ]; then
    echo "[estargz-bench] leg-window MARK lines absent; skipping (run_e2e.sh out of date?)" >&2
else
    origin_report=$(tail -n +$(( testfs_start + 1 )) "${testfs_log}" 2>/dev/null \
        | python3 -c '
import sys, json, calendar, time

def epoch(tok):
    tok = tok.strip().rstrip("Z")
    base, _, frac = tok.partition(".")
    e = calendar.timegm(time.strptime(base, "%Y-%m-%dT%H:%M:%S"))
    return e + (float("0." + frac) if frac else 0.0)

full_img = float(sys.argv[1])
legs = [
    ("overlayfs (full pull)", float(sys.argv[2]), float(sys.argv[3])),
    ("stargz (lazy)", float(sys.argv[4]), float(sys.argv[5])),
]
agg = {name: [0, 0, 0, 0, 0, 0] for name, _, _ in legs}
for line in sys.stdin:
    if "testfs download" not in line:
        continue
    head = line.split(None, 1)
    if not head:
        continue
    try:
        ts = epoch(head[0])
    except Exception:
        continue
    leg = next((n for n, s, e in legs if s <= ts <= e), None)
    if leg is None:
        continue
    i = line.find("{")
    if i < 0:
        continue
    try:
        ev = json.loads(line[i:])
    except Exception:
        continue
    name = ev.get("name", "")
    status = ev.get("status")
    nbytes = int(ev.get("bytes", 0))
    a = agg[leg]
    if name.endswith(".kmeta"):
        a[0] += 1; a[1] += nbytes
    elif status == 206:
        a[2] += 1; a[3] += nbytes
    elif status == 200:
        a[4] += 1; a[5] += nbytes
mib = lambda b: b / 1048576.0
if full_img > 0:
    print(f"full converted image     : {mib(full_img):.1f} MiB ({int(full_img)} bytes)")
for name, _, _ in legs:
    side_n, side_b, pn, pb, fn, fb = agg[name]
    tot = side_b + pb + fb
    print(f"-- {name} --")
    print(f"   sidecar (.kmeta)      : {side_n} GETs, {side_b} bytes")
    print(f"   piece range (206)     : {pn} GETs, {mib(pb):.1f} MiB")
    print(f"   full blob (200)       : {fn} GETs, {mib(fb):.1f} MiB")
    print(f"   TOTAL origin->testfs  : {mib(tot):.1f} MiB ({tot} bytes)")
    if full_img > 0:
        print(f"   share of full image   : {100.0 * tot / full_img:.1f}%")
' "${overlay_full_bytes:-0}" "${ov_start}" "${ov_end}" "${sg_start}" "${sg_end}" \
        2>/dev/null || true)
    if [ -n "${origin_report}" ]; then
        echo "${origin_report}" >&2
    else
        echo "no 'testfs download' lines in either leg window (origin may not have been cold)" >&2
    fi
fi
echo "overlayfs full pull from a COLD origin approaches ~100% of the image;" >&2
echo "stargz lazy stays far below it -- the cold-origin full-vs-lazy proof." >&2
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
