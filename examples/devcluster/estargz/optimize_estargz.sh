#!/bin/bash
# Convert an image to eStargz using ctr-remote optimize, using the container's
# actual entrypoint/env from crictl inspect output to build a prefetch profile.
# Usage: ./optimize_estargz.sh <src_image> <dst_image> [inspect_json]
#   src_image     e.g. 127.0.0.1:5055/uber-usi/ma-endpoint-...:bkt1-produ-...
#   dst_image     e.g. 127.0.0.1:5055/uber-usi/ma-endpoint-...:estargz
#   inspect_json  crictl inspect output (default: output.json in this dir)
#
# The entrypoint, args, env, and working dir are extracted from the inspect
# JSON and passed to ctr-remote optimize --period so startup file accesses
# are recorded into the eStargz prefetch profile.

set -eu

SRC="${1:?usage: $0 <src_image> <dst_image> [inspect_json]}"
DST="${2:?usage: $0 <src_image> <dst_image> [inspect_json]}"
INSPECT="${3:-$(dirname "$0")/../../output.json}"
PERIOD="${PERIOD:-30}"

if ! command -v ctr-remote &>/dev/null; then
    echo "ctr-remote not found — install from stargz-snapshotter releases"
    exit 1
fi

if [ ! -f "$INSPECT" ]; then
    echo "inspect JSON not found: $INSPECT"
    exit 1
fi

# Extract entrypoint args from runtimeSpec.process.args
ARGS=$(python3 - "$INSPECT" << 'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
args = data["info"]["runtimeSpec"]["process"]["args"]
# JSON-encode as array string for ctr-remote
print(json.dumps(args))
PYEOF
)

# Extract entrypoint (first element) and rest of args
ENTRYPOINT=$(python3 -c "import json,sys; a=json.loads(sys.argv[1]); print(json.dumps([a[0]]))" "$ARGS")
ARGS_REST=$(python3 -c "import json,sys; a=json.loads(sys.argv[1]); print(json.dumps(a[1:]))" "$ARGS")

# Extract key env vars needed for startup (skip runtime-injected ones)
ENV_FLAGS=$(python3 - "$INSPECT" << 'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
# From config.envs (scheduler-injected), pick the ones the entrypoint needs
wanted = {
    "UDEPLOY_APP_ID", "SECRETS_PATH", "KUBERNETES_POD_NAME",
    "UBER_ENVIRONMENT", "UBER_PARTITION", "UDEPLOY_SERVICE_NAME",
}
flags = []
for e in data["info"]["config"].get("envs", []):
    if e["key"] in wanted:
        flags.append(f'--env={e["key"]}={e["value"]}')
# Provide dummy secrets path so entrypoint doesn't fail immediately
if not any("SECRETS_PATH" in f for f in flags):
    flags.append("--env=SECRETS_PATH=/tmp/secrets")
if not any("KUBERNETES_POD_NAME" in f for f in flags):
    flags.append("--env=KUBERNETES_POD_NAME=optimize-probe")
print("\n".join(flags))
PYEOF
)

# Extract working dir
CWD=$(python3 - "$INSPECT" << 'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
print(data["info"]["runtimeSpec"]["process"].get("cwd", "/"))
PYEOF
)

echo "src:        $SRC"
echo "dst:        $DST"
echo "entrypoint: $ENTRYPOINT"
echo "args:       $ARGS_REST"
echo "cwd:        $CWD"
echo "period:     ${PERIOD}s"
echo

# Build env flag array
ENV_ARGS=()
while IFS= read -r line; do
    [ -n "$line" ] && ENV_ARGS+=("$line")
done <<< "$ENV_FLAGS"

ctr-remote image optimize --oci \
    --zstdchunked \
    --zstdchunked-compression-level=15 \
    --period="$PERIOD" \
    --cwd="$CWD" \
    "${ENV_ARGS[@]}" \
    --entrypoint="$ENTRYPOINT" \
    --args="$ARGS_REST" \
    --mount "type=tmpfs,destination=/var/cache/data" \
    --mount "type=tmpfs,destination=/mnt/sandbox" \
    --mount "type=tmpfs,destination=/mnt/mesos" \
    --mount "type=tmpfs,destination=/mnt/tmp" \
    "$SRC" "$DST"
