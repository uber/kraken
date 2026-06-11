#!/bin/bash
# Agent-to-agent P2P proof for the streaming read path.
#
# The single-agent stream_benchmark.sh shows ?stream=1 lowers TTFB, but it does
# not show whether streamed pieces actually move agent<->agent (Kraken's whole
# point) or just origin->agent. This benchmark proves the former:
#
#   1. agent-one streams a fresh blob cold  -> only the origin seeds it, so every
#      piece agent-one receives comes FROM the origin. agent-one's own peer id is
#      read from its network-event log.
#   2. agent-two streams the SAME blob      -> now both the origin AND agent-one
#      hold it. We tally agent-two's receive_piece events by source peer:
#        - pieces sourced from agent-one's peer id == agent<->agent P2P, while
#          streaming, with no Kraken core change.
#        - pieces sourced from any other peer      == origin.
#
# receive_piece events (event/self/peer/piece JSON) require network_event in
# config/agent/development.yaml (enabled by this harness's config). The agents
# are always restarted so the log starts fresh and picks up that config.
#
# Usage:
#   examples/devcluster/p2p_agent_benchmark.sh [size_mb ...]   # default: 256
#
# Env:
#   SKIP_CLUSTER=1   reuse a running devcluster (skip `make devcluster`)
#   READY_TIMEOUT=N  seconds to wait for components (default 180)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

SIZES_MB=("$@")
if [ "${#SIZES_MB[@]}" -eq 0 ]; then
    SIZES_MB=(256)
fi

TESTFS=localhost:14000
ORIGIN=localhost:15002
AGENT_ONE=localhost:16002
AGENT_TWO=localhost:17002
NS=testfs
NETLOG=/var/log/kraken/kraken-agent/networkevent.log
READY_TIMEOUT="${READY_TIMEOUT:-180}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

log() { echo "[p2p-bench] $*" >&2; }

wait_ready() {
    local name=$1 url=$2 deadline
    deadline=$(( $(date +%s) + READY_TIMEOUT ))
    until curl -sf -o /dev/null "${url}"; do
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            log "ERROR: ${name} not ready after ${READY_TIMEOUT}s"; return 1
        fi
        sleep 1
    done
}

# netlog_lines CONTAINER: current line count of the agent's network-event log.
netlog_lines() { docker exec "$1" sh -c "wc -l < ${NETLOG} 2>/dev/null || echo 0" | tr -d ' '; }

# self_peer_id CONTAINER: the agent's own peer id (the 'self' field it stamps on
# every event it emits).
self_peer_id() {
    docker exec "$1" sh -c "head -n1 ${NETLOG} 2>/dev/null" \
        | python3 -c 'import sys,json
line=sys.stdin.read().strip()
print(json.loads(line)["self"] if line else "")'
}

# --- Start / restart the cluster --------------------------------------------

if [ "${SKIP_CLUSTER:-0}" = "1" ]; then
    log "SKIP_CLUSTER=1: restarting only the agents to apply network_event config"
    docker rm -fv kraken-agent-one kraken-agent-two >/dev/null 2>&1 || true
    ./examples/devcluster/agent_one_start_container.sh >/dev/null 2>&1
    ./examples/devcluster/agent_two_start_container.sh >/dev/null 2>&1
else
    log "starting devcluster (make devcluster)"
    make devcluster
fi

wait_ready testfs    "http://${TESTFS}/health"
wait_ready origin    "http://${ORIGIN}/health"
wait_ready agent-one "http://${AGENT_ONE}/readiness"
wait_ready agent-two "http://${AGENT_TWO}/readiness"

# --- Run --------------------------------------------------------------------

declare -a ROWS
for mb in "${SIZES_MB[@]}"; do
    blob="${WORKDIR}/blob_${mb}mb.bin"
    dd if=/dev/urandom of="${blob}" bs=1048576 count="${mb}" status=none
    hex="$(sha256sum "${blob}" | awk '{print $1}')"
    log "size=${mb}MB digest=${hex:0:12}"

    log "  uploading to testfs (origin + both agents cold)"
    curl -sf -X POST -T "${blob}" "http://${TESTFS}/files/blobs/${hex}" >/dev/null

    one_out="${WORKDIR}/one_${mb}.out"
    two_out="${WORKDIR}/two_${mb}.out"

    log "  agent-one streams (cold; only origin can seed)"
    curl -s -o "${one_out}" "http://${AGENT_ONE}/namespace/${NS}/blobs/${hex}?stream=1"

    # Snapshot agent-two's event log AFTER agent-one seeds, so we attribute only
    # this leg's pieces.
    two_start="$(netlog_lines kraken-agent-two)"

    log "  agent-two streams the same blob (origin + agent-one available)"
    curl -s -o "${two_out}" "http://${AGENT_TWO}/namespace/${NS}/blobs/${hex}?stream=1"

    # Integrity.
    for f in "${one_out}" "${two_out}"; do
        got="$(sha256sum "${f}" | awk '{print $1}')"
        [ "${got}" = "${hex}" ] || { log "ERROR: digest mismatch on ${f}"; exit 1; }
    done

    agent_one_id="$(self_peer_id kraken-agent-one)"
    [ -n "${agent_one_id}" ] || { log "ERROR: could not read agent-one peer id"; exit 1; }

    # Tally agent-two's receive_piece events from this leg by source peer.
    stats="$(docker exec kraken-agent-two sh -c "tail -n +$((two_start + 1)) ${NETLOG}" \
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
' "${agent_one_id}")"

    read -r from_a1 from_origin total pct <<<"${stats}"
    log "  agent-two pieces: ${from_a1} from agent-one (P2P), ${from_origin} from origin"
    ROWS+=("$(printf '%-8s %14s %14s %10s %8s' \
        "${mb}MB" "${from_a1}" "${from_origin}" "${total}" "${pct}%")")
done

# --- Results ----------------------------------------------------------------

echo
echo "============== agent-to-agent P2P during streaming =============="
printf '%-8s %14s %14s %10s %8s\n' \
    "size" "from_agent1" "from_origin" "pieces" "p2p%"
for row in "${ROWS[@]}"; do echo "${row}"; done
echo "================================================================"
echo "from_agent1 = receive_piece events agent-two sourced from agent-one"
echo "from_origin = receive_piece events agent-two sourced from the origin"
echo "p2p%        = share of streamed pieces that came peer->peer"
echo "any from_agent1 > 0 proves agent<->agent P2P works while streaming"
