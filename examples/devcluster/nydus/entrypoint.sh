#!/bin/bash
# Entrypoint for the nydus e2e DinD container: starts containerd-nydus-grpc and
# containerd, waits until both are healthy, then -- if given benchmark args --
# runs run_e2e.sh as the container workload (output to stdout/docker logs); with
# no args it idles so the host can drive it manually via docker exec.
#
# nydusd is NOT started here: in daemon_mode="dedicated" the snapshotter launches
# one nydusd per image mount, using nydusd_path/nydusd_config from
# nydus_snapshotter.toml.

set -ex

mkdir -p /run/containerd-nydus /var/log/kraken-nydus \
    /var/lib/containerd /var/lib/nydus/cache

# Registry config so nerdctl/ctr reach Kraken over plain HTTP: the proxy (push)
# and both agent registries (pull) used by the A/B legs. Addressed as 127.0.0.1
# because the container shares the host network namespace; nydusd's own registry
# backend uses plain HTTP via scheme="http" in nydusd-config.fusedev.json.
for hostport in \
    127.0.0.1:15000 \
    127.0.0.1:16000 \
    127.0.0.1:17000; do
    dir="/etc/containerd/certs.d/${hostport}"
    mkdir -p "${dir}"
    cat >"${dir}/hosts.toml" <<EOF
server = "http://${hostport}"

[host."http://${hostport}"]
  capabilities = ["pull", "resolve", "push"]
  skip_verify = true
EOF
done

containerd-nydus-grpc \
    --config /etc/nydus/config.toml \
    --log-level info \
    --log-to-stdout \
    >/var/log/kraken-nydus/nydus-snapshotter.log 2>&1 &
nydus_pid=$!

containerd >/var/log/kraken-nydus/containerd.log 2>&1 &
ctrd_pid=$!

# Wait for containerd's socket, failing loudly with logs if it dies.
ready=0
for _ in $(seq 1 60); do
    if ctr version >/dev/null 2>&1; then ready=1; break; fi
    if ! kill -0 "${ctrd_pid}" 2>/dev/null; then break; fi
    sleep 0.5
done

if [ "${ready}" -ne 1 ]; then
    echo "ERROR: containerd did not become ready" >&2
    echo "===== containerd.log =====" >&2; cat /var/log/kraken-nydus/containerd.log >&2
    echo "===== nydus-snapshotter.log =====" >&2; cat /var/log/kraken-nydus/nydus-snapshotter.log >&2
    exit 1
fi

if ! kill -0 "${nydus_pid}" 2>/dev/null; then
    echo "ERROR: containerd-nydus-grpc exited during startup" >&2
    cat /var/log/kraken-nydus/nydus-snapshotter.log >&2
    exit 1
fi

ctr plugin ls | grep -i nydus || true
echo "kraken-nydus ready: containerd + containerd-nydus-grpc running"

# With benchmark args, run the e2e comparison as the container's workload so its
# full report goes to stdout (== docker logs); the host reads it with
# `docker logs -f` instead of `docker exec` (no connection upgrade needed). With
# no args, idle so the container can be driven manually.
if [ "$#" -gt 0 ]; then
    set +x
    exec /usr/local/bin/run_e2e.sh "$@"
fi

# Keep the container alive for manual docker exec-driven runs.
tail -f /var/log/kraken-nydus/containerd.log
