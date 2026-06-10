#!/usr/bin/env bash
#
# Phase 1 streaming PoC A/B: time-to-first-byte (TTFB) of a blob served by the
# Kraken agent, baseline (whole-blob, blocking) vs ?stream=1 (piece-streaming).
#
# Procedure per size: upload a fresh random blob to testfs, warm the ORIGIN
# (so backend-fetch + metainfo cost is excluded and common to both modes), then
# measure agent-one (baseline, cold cache) vs agent-two (stream, cold cache),
# both leeching from the warm origin.
set -euo pipefail

TESTFS=localhost:14000
ORIGIN=localhost:15002
AGENT_ONE=localhost:16002   # baseline (no stream)
AGENT_TWO=localhost:17002   # streaming (?stream=1)
SIZES_MB="${SIZES_MB:-128 256 512}"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

make_blob() {
  local mb="$1" f="$TMP/b_${mb}_$RANDOM.bin"
  dd if=/dev/urandom of="$f" bs=1M count="$mb" status=none
  local h; h="$(sha256sum "$f" | awk '{print $1}')"
  # -T streams the upload from disk (POST), avoiding curl buffering large blobs.
  curl -sf -X POST -T "$f" "http://$TESTFS/files/blobs/$h" -o /dev/null
  echo "$h"
}

warm_origin() {   # poll until origin has fully cached + can serve the blob (200)
  local h="$1"
  for _ in $(seq 1 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://$ORIGIN/namespace/testfs/blobs/sha256:$h")"
    [ "$code" = "200" ] && return 0
    sleep 1
  done
  echo "WARN: origin did not warm for $h" >&2; return 1
}

fetch() {  # <agent> <digest> <stream?> -> "ttfb total"
  local agent="$1" h="$2" stream="$3"
  local url="http://$agent/namespace/testfs/blobs/sha256:$h"
  [ "$stream" = "1" ] && url="$url?stream=1"
  curl -s -o /dev/null -w '%{time_starttransfer} %{time_total}' "$url"
}

printf '%-7s | %-23s | %-23s | %s\n' "size" "baseline (blocking)" "stream (?stream=1)" "TTFB speedup"
printf '%-7s | %-11s %-11s | %-11s %-11s |\n' "" "ttfb(s)" "total(s)" "ttfb(s)" "total(s)"
echo "--------+-------------------------+-------------------------+-------------"
for mb in $SIZES_MB; do
  h="$(make_blob "$mb")"; warm_origin "$h" || true
  b_res="$(fetch "$AGENT_ONE" "$h" "0")"; b_ttfb="${b_res% *}"; b_total="${b_res#* }"
  s_res="$(fetch "$AGENT_TWO" "$h" "1")"; s_ttfb="${s_res% *}"; s_total="${s_res#* }"
  speedup="$(awk -v b="$b_ttfb" -v s="$s_ttfb" 'BEGIN{ if (s>0) printf "%.1fx", b/s; else print "n/a" }')"
  printf '%-7s | %-11s %-11s | %-11s %-11s | %s\n' \
    "${mb}MB" "$b_ttfb" "$b_total" "$s_ttfb" "$s_total" "$speedup"
done
