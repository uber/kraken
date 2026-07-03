#!/bin/bash
IMAGE="$1"
SNAPSHOTTER="${3:-}"
INSPECT="${2:-$(dirname "$0")/../../output.json}"
STARGZ_ROOT="${STARGZ_ROOT:-/var/lib/containerd-stargz-grpc}"
CLEAR_STARGZ_CACHE="1"

if [ ! -f "$INSPECT" ]; then
    echo "inspect JSON not found: $INSPECT"
    exit 1
fi

unmount_stale() {
    local root="$1"
    [ -d "$root" ] || return 0
    mount | awk -v r="$root" '$3 ~ ("^" r) {print $3}' | tac | while read -r mp; do
        sudo umount -l "$mp" 2>/dev/null
    done
}

cleanup() {
    nerdctl rm -f measure-probe >/dev/null 2>&1 || true
    unmount_stale "$STARGZ_ROOT/snapshotter/snapshots"

    for lease in $(sudo ctr -n default leases ls 2>/dev/null | awk 'NR>1{print $1}'); do
        sudo ctr -n default leases rm "$lease" >/dev/null 2>&1
    done

    for img in $(sudo ctr -n default images ls -q 2>/dev/null); do
        sudo ctr -n default images rm "$img" >/dev/null 2>&1
    done
    for snap in $(sudo ctr -n default snapshots ls -q 2>/dev/null); do
        sudo ctr -n default snapshots rm "$snap" >/dev/null 2>&1
    done
    for c in $(sudo ctr -n default content ls -q 2>/dev/null); do
        sudo ctr -n default content rm "$c" >/dev/null 2>&1
    done

    AGENT_CONTAINER=$(sudo docker ps --format '{{.Names}}' | grep -m1 '^kraken-agent')
    if [ -n "$AGENT_CONTAINER" ]; then
        sudo docker exec "$AGENT_CONTAINER" sh -c 'rm -rf /var/cache/udocker/kraken-agent/cache/* /var/cache/udocker/kraken-agent/download/*'
        sudo docker restart "$AGENT_CONTAINER" >/dev/null
        for ((i=0; i<60; i++)); do
            curl -sf -o /dev/null http://127.0.0.1:5055/v2/ && break
            sleep 1
        done
    fi

    if [ "$SNAPSHOTTER" = "stargz" ] && [ "$CLEAR_STARGZ_CACHE" = "1" ]; then
        sudo systemctl stop stargz-snapshotter
        sudo rm -rf "${STARGZ_ROOT:?}/stargz/httpcache" "${STARGZ_ROOT:?}/stargz/fscache" "${STARGZ_ROOT:?}/snapshotter"
        sudo systemctl start stargz-snapshotter
        sleep 1
    fi
}

trap cleanup EXIT
cleanup

ARGS_JSON=$(python3 - "$INSPECT" << 'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
print(json.dumps(data["info"]["runtimeSpec"]["process"]["args"]))
PYEOF
)

CWD=$(python3 - "$INSPECT" << 'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
print(data["info"]["runtimeSpec"]["process"].get("cwd", "/"))
PYEOF
)

ENV_FLAGS=$(python3 - "$INSPECT" << 'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
wanted = {
    "UDEPLOY_APP_ID", "SECRETS_PATH", "KUBERNETES_POD_NAME",
    "UBER_ENVIRONMENT", "UBER_PARTITION", "UDEPLOY_SERVICE_NAME",
}
flags = []
for e in data["info"]["config"].get("envs", []):
    if e["key"] in wanted:
        flags.append(f'--env={e["key"]}={e["value"]}')
if not any("SECRETS_PATH" in f for f in flags):
    flags.append("--env=SECRETS_PATH=/tmp/secrets")
if not any("KUBERNETES_POD_NAME" in f for f in flags):
    flags.append("--env=KUBERNETES_POD_NAME=measure-probe")
print("\n".join(flags))
PYEOF
)

ENTRYPOINT_CMD=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])[0])" "$ARGS_JSON")

ENV_ARGS=()
while IFS= read -r line; do
    [ -n "$line" ] && ENV_ARGS+=("$line")
done <<< "$ENV_FLAGS"

readarray -t CMD_ARGS < <(python3 -c "
import json, sys
for a in json.loads(sys.argv[1])[1:]:
    print(a)
" "$ARGS_JSON")

nerdctl ${SNAPSHOTTER:+--snapshotter $SNAPSHOTTER} rmi "${IMAGE}" >/dev/null 2>&1 || true

RUN_CMD=(nerdctl)
[ -n "$SNAPSHOTTER" ] && RUN_CMD+=(--snapshotter "$SNAPSHOTTER")
RUN_CMD+=(run
    --name measure-probe
    --pull always
    --net none
    --workdir="$CWD"
    "${ENV_ARGS[@]}"
    --entrypoint "$ENTRYPOINT_CMD"
    --mount "type=tmpfs,destination=/var/cache/data"
    --mount "type=tmpfs,destination=/mnt/sandbox"
    --mount "type=tmpfs,destination=/mnt/mesos"
    --mount "type=tmpfs,destination=/mnt/tmp"
    "${IMAGE}"
    "${CMD_ARGS[@]}")

printf 'run command:\n'
last=$((${#RUN_CMD[@]} - 1))
for i in "${!RUN_CMD[@]}"; do
    if [ "$i" -eq "$last" ]; then
        printf '  %q\n' "${RUN_CMD[$i]}"
    else
        printf '  %q \\\n' "${RUN_CMD[$i]}"
    fi
done

START=$(date +%s%3N)
"${RUN_CMD[@]}" >/dev/null
echo "container created: $(( $(date +%s%3N) - START ))ms"

STATUS=""
for ((i=0; i<300; i++)); do
    STATUS=$(nerdctl inspect -f '{{.State.Status}}' measure-probe 2>/dev/null)
    [ "$STATUS" = "running" ] && break
    sleep 1
done
echo "container running: $(( $(date +%s%3N) - START ))ms (status=$STATUS)"

DECLARED_SIZE=$(python3 - "$IMAGE" << 'PYEOF'
import json, sys, urllib.request

ref = sys.argv[1]
host, rest = ref.split("/", 1)
repo, tag = rest.rsplit(":", 1)
url = f"http://{host}/v2/{repo}/manifests/{tag}"
req = urllib.request.Request(url, headers={
    "Accept": "application/vnd.oci.image.manifest.v1+json,"
              "application/vnd.docker.distribution.manifest.v2+json",
})
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        manifest = json.load(resp)
    total = manifest.get("config", {}).get("size", 0)
    total += sum(layer.get("size", 0) for layer in manifest.get("layers", []))
    print(total)
except Exception as e:
    print(f"unknown ({e})")
PYEOF
)
echo "declared image size: ${DECLARED_SIZE} bytes ($(python3 -c "print(f'{${DECLARED_SIZE:-0} / 1e9:.2f}')" 2>/dev/null || echo "?") GB)"

if [ "$SNAPSHOTTER" = "stargz" ]; then
    FETCHED_BYTES=$(sudo du -sb "$STARGZ_ROOT/stargz/httpcache" 2>/dev/null | cut -f1)
    echo "bytes fetched (stargz httpcache): ${FETCHED_BYTES:-0} bytes ($(python3 -c "print(f'{${FETCHED_BYTES:-0} / 1e9:.2f}')" 2>/dev/null || echo "?") GB)"
fi
