#!/bin/bash
# Entrypoint for the soci e2e DinD container: starts the soci-snapshotter grpc
# plugin and containerd, waits until both are healthy, then -- if given benchmark
# args -- runs run_e2e.sh as the container workload (output to stdout/docker
# logs); with no args it idles so the host can drive it manually via docker exec.

set -ex

mkdir -p /run/soci-snapshotter-grpc /var/log/kraken-soci \
    /var/lib/containerd /var/lib/soci-snapshotter-grpc

# Registry config so nerdctl/ctr reach Kraken over plain HTTP: the proxy (push)
# and both agent registries (pull) used by the A/B legs. Addressed as 127.0.0.1
# because soci-snapshotter only uses plain HTTP for localhost hosts (see
# soci_start_container.sh); the container shares the host network namespace.
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

soci-snapshotter-grpc \
    --config /etc/soci-snapshotter-grpc/config.toml \
    >/var/log/kraken-soci/soci-snapshotter.log 2>&1 &
soci_pid=$!

containerd >/var/log/kraken-soci/containerd.log 2>&1 &
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
    echo "===== containerd.log =====" >&2; cat /var/log/kraken-soci/containerd.log >&2
    echo "===== soci-snapshotter.log =====" >&2; cat /var/log/kraken-soci/soci-snapshotter.log >&2
    exit 1
fi

if ! kill -0 "${soci_pid}" 2>/dev/null; then
    echo "ERROR: soci-snapshotter exited during startup" >&2
    cat /var/log/kraken-soci/soci-snapshotter.log >&2
    exit 1
fi

ctr plugin ls | grep -i soci || true
echo "kraken-soci ready: containerd + soci-snapshotter running"

# With benchmark args, run the e2e comparison as the container's workload so its
# full report goes to stdout (== docker logs); the host reads it with
# `docker logs -f` instead of `docker exec` (no connection upgrade needed). With
# no args, idle so the container can be driven manually.
if [ "$#" -gt 0 ]; then
    set +x
    exec /usr/local/bin/run_e2e.sh "$@"
fi

# Keep the container alive for manual docker exec-driven runs.
tail -f /var/log/kraken-soci/containerd.log
