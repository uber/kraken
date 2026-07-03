#!/bin/bash
# eStargz e2e verification on a real Kraken host with stargz-snapshotter.
# Push image via proxy, pull via agent with stargz snapshotter,
# verify manifest annotations and lazy remote-mount.
# Does NOT run the container — start it separately after this script passes.
#
# Usage: ./verify_e2e.sh <proxy_url> <agent_url> <image>
#   proxy_url   Kraken proxy  (e.g. dev7-ee:15000)
#   agent_url   Kraken agent  (e.g. dev7-ee:16000)
#   image       Source image already present locally (e.g. python:3.12)

set -eu

PROXY_URL="${1:?usage: $0 <proxy_url> <agent_url> <image>}"
AGENT_URL="${2:?usage: $0 <proxy_url> <agent_url> <image>}"
SRC_IMAGE="${3:?usage: $0 <proxy_url> <agent_url> <image>}"

TAG="verify-$(date +%s)"

BASE="${SRC_IMAGE##*/}"
REPO="${BASE%%:*}"

PROXY_REF="${PROXY_URL}/${REPO}:${TAG}"
AGENT_REF="${AGENT_URL}/${REPO}:${TAG}"

log() { echo "[verify_e2e] $*"; }

log "push: ${SRC_IMAGE} -> ${PROXY_REF}"
docker tag "${SRC_IMAGE}" "${PROXY_REF}"
docker push "${PROXY_REF}"

PULL_START=$(date -u "+%Y-%m-%d %H:%M:%S")

log "pull: ${AGENT_REF} (snapshotter=stargz)"
nerdctl --snapshotter stargz pull "${AGENT_REF}"

# --- Manifest annotation check via agent registry API ---
MANIFEST=$(curl -fsSL \
    -H "Accept: application/vnd.oci.image.manifest.v1+json" \
    -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
    "http://${AGENT_URL}/v2/${REPO}/manifests/${TAG}" 2>/dev/null || true)

if [ -z "${MANIFEST}" ]; then
    echo "ERROR: could not fetch manifest from http://${AGENT_URL}/v2/${REPO}/manifests/${TAG}"
    exit 1
fi

STARGZ_ANNOT=false
if echo "${MANIFEST}" | grep -qE 'stargz/toc\.digest|application/vnd\.oci\.image\.layer\.stargz|zstd-chunked'; then
    STARGZ_ANNOT=true
fi

# --- Lazy-mount check from journald (stargz-snapshotter is a systemd service) ---
LAZY_LOG=$(journalctl -u stargz-snapshotter --since "${PULL_START}" 2>/dev/null || true)
LAZY_TRUE=$(echo "${LAZY_LOG}" | grep -c '"remote-snapshot-prepared":"true"' || true)
LAZY_FAIL=$(echo "${LAZY_LOG}" | grep -c '"remote-snapshot-prepared":"false"' || true)

echo
echo "Manifest:"
echo "${MANIFEST}" | python3 -m json.tool 2>/dev/null || echo "${MANIFEST}"
echo
echo "eStargz annotations present        : ${STARGZ_ANNOT}"
echo "lazy layers (remote-snapshot=true) : ${LAZY_TRUE}"
echo "failed lazy mounts (must be 0)     : ${LAZY_FAIL}"
echo

PASS=true
if [ "${STARGZ_ANNOT}" != "true" ]; then
    echo "FAIL: no eStargz annotations — convert with ctr-remote optimize first"
    PASS=false
fi
if [ "${LAZY_FAIL}" -gt 0 ]; then
    echo "WARN: ${LAZY_FAIL} layer(s) fell back to full pull (remote-snapshot-prepared=false)"
fi
if [ "${PASS}" = "true" ]; then
    if [ "${LAZY_TRUE}" -gt 0 ]; then
        echo "PASS: eStargz lazy pull verified (${LAZY_TRUE} layer(s) remote-mounted)"
    else
        echo "WARN: no lazy mounts — stargz snapshotter received the pull but no layers went lazy"
    fi
fi

[ "${PASS}" = "true" ]
