#!/bin/bash
# Starts the Kraken devcluster and runs an A/B time-to-first-byte (TTFB)
# benchmark of the agent blob endpoint:
#
#   before: baseline blocking download  (agent-one :16002, no ?stream)
#   after:  streaming download          (agent-two :17002, ?stream=1)
#
# Both agents share the same in-order piece policy (config/agent/development.yaml),
# so the only variable between the two legs is the ?stream=1 toggle. Each size
# uses a freshly generated random blob uploaded to the testfs backend, so both
# agents (and the origin) start cold for that digest.
#
# Usage:
#   examples/devcluster/stream_benchmark.sh [size_mb ...]      # default: 64 256 512
#
# Env:
#   SKIP_CLUSTER=1     reuse an already-running devcluster (skip `make devcluster`)
#   READY_TIMEOUT=N    seconds to wait for components to come up (default 180)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

SIZES_MB=("$@")
if [ "${#SIZES_MB[@]}" -eq 0 ]; then
    SIZES_MB=(64 256 512)
fi

TESTFS=localhost:14000
ORIGIN=localhost:15002
AGENT_BASELINE=localhost:16002
AGENT_STREAM=localhost:17002
NS=testfs
READY_TIMEOUT="${READY_TIMEOUT:-180}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

log() { echo "[bench] $*" >&2; }

# wait_ready NAME URL: poll URL until it returns 2xx or the timeout elapses.
wait_ready() {
    local name=$1 url=$2 deadline
    deadline=$(( $(date +%s) + READY_TIMEOUT ))
    log "waiting for ${name} (${url})"
    until curl -sf -o /dev/null "${url}"; do
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            log "ERROR: ${name} not ready after ${READY_TIMEOUT}s"
            return 1
        fi
        sleep 1
    done
}

# upload_blob HEX PATH: push a blob to the testfs backend (cold remote storage).
upload_blob() {
    local hex=$1 path=$2
    curl -sf -X POST -T "${path}" "http://${TESTFS}/files/blobs/${hex}"
}

# download URL OUT: GET URL to OUT, echo "TTFB_SECONDS TOTAL_SECONDS".
download() {
    local url=$1 out=$2
    curl -s -o "${out}" -w '%{time_starttransfer} %{time_total}' "${url}"
}

# --- Start the cluster -------------------------------------------------------

if [ "${SKIP_CLUSTER:-0}" = "1" ]; then
    log "SKIP_CLUSTER=1: reusing the running devcluster"
else
    log "starting devcluster (make devcluster)"
    make devcluster
fi

wait_ready testfs   "http://${TESTFS}/health"
wait_ready origin   "http://${ORIGIN}/health"
wait_ready agent-one "http://${AGENT_BASELINE}/readiness"
wait_ready agent-two "http://${AGENT_STREAM}/readiness"

# --- Run the benchmark -------------------------------------------------------

declare -a ROWS
for mb in "${SIZES_MB[@]}"; do
    blob="${WORKDIR}/blob_${mb}mb.bin"
    dd if=/dev/urandom of="${blob}" bs=1048576 count="${mb}" status=none
    hex="$(sha256sum "${blob}" | awk '{print $1}')"
    log "size=${mb}MB digest=${hex}"

    log "  uploading to testfs"
    upload_blob "${hex}" "${blob}"

    base_out="${WORKDIR}/base_${mb}.out"
    stream_out="${WORKDIR}/stream_${mb}.out"

    log "  baseline  download (blocking)  ${AGENT_BASELINE}"
    read -r base_ttfb base_total < <(
        download "http://${AGENT_BASELINE}/namespace/${NS}/blobs/${hex}" "${base_out}")

    log "  streaming download (?stream=1) ${AGENT_STREAM}"
    read -r stream_ttfb stream_total < <(
        download "http://${AGENT_STREAM}/namespace/${NS}/blobs/${hex}?stream=1" "${stream_out}")

    # Integrity: streamed and baseline bytes must both match the digest.
    base_got="$(sha256sum "${base_out}" | awk '{print $1}')"
    stream_got="$(sha256sum "${stream_out}" | awk '{print $1}')"
    if [ "${base_got}" != "${hex}" ]; then
        log "ERROR: baseline body digest mismatch (${base_got} != ${hex})"; exit 1
    fi
    if [ "${stream_got}" != "${hex}" ]; then
        log "ERROR: streamed body digest mismatch (${stream_got} != ${hex})"; exit 1
    fi

    speedup="$(awk -v b="${base_ttfb}" -v s="${stream_ttfb}" \
        'BEGIN { if (s > 0) printf "%.1fx", b / s; else printf "n/a" }')"
    ROWS+=("$(printf '%-8s %12s %12s %12s %12s %10s' \
        "${mb}MB" "${base_ttfb}" "${base_total}" "${stream_ttfb}" "${stream_total}" "${speedup}")")
done

# --- Results -----------------------------------------------------------------

echo
echo "=================== blob TTFB: before vs after ==================="
printf '%-8s %12s %12s %12s %12s %10s\n' \
    "size" "base_ttfb" "base_total" "strm_ttfb" "strm_total" "ttfb_x"
for row in "${ROWS[@]}"; do
    echo "${row}"
done
echo "================================================================="
echo "base_*  = blocking download (agent :16002)"
echo "strm_*  = streaming ?stream=1 (agent :17002)"
echo "ttfb_x  = baseline TTFB / streaming TTFB (higher is better)"
echo "all times in seconds"
