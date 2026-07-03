#!/bin/bash
# Set up nerdctl for pushing to a Kraken proxy on a real host.
# Usage: ./setup_nerdctl_push.sh <proxy_addr>
#   proxy_addr  e.g. 127.0.0.1:5055
set -eu

PROXY_ADDR="${1:?usage: $0 <proxy_addr>}"

# --- Install nerdctl ---
if ! command -v nerdctl &>/dev/null; then
    echo "installing nerdctl..."
    curl -Lo /tmp/nerdctl.tar.gz https://github.com/containerd/nerdctl/releases/download/v2.0.4/nerdctl-2.0.4-linux-amd64.tar.gz
    tar xf /tmp/nerdctl.tar.gz -C /usr/local/bin/ nerdctl
    echo "nerdctl installed: $(nerdctl --version)"
else
    echo "nerdctl already installed: $(nerdctl --version)"
fi

# --- Detect HTTP vs HTTPS ---
SCHEME="https"
if curl -sk "https://${PROXY_ADDR}/v2/" -o /dev/null; then
    SCHEME="https"
    echo "detected HTTPS on ${PROXY_ADDR}"
else
    SCHEME="http"
    echo "detected HTTP on ${PROXY_ADDR}"
fi

# --- Create hosts.toml ---
HOSTS_DIR="/etc/containerd/certs.d/${PROXY_ADDR}"
mkdir -p "${HOSTS_DIR}"
if [ "${SCHEME}" = "https" ]; then
    cat > "${HOSTS_DIR}/hosts.toml" << EOF
server = "https://${PROXY_ADDR}"

[host."https://${PROXY_ADDR}"]
  capabilities = ["pull", "push", "resolve"]
  skip_verify = true
EOF
else
    cat > "${HOSTS_DIR}/hosts.toml" << EOF
server = "http://${PROXY_ADDR}"

[host."http://${PROXY_ADDR}"]
  capabilities = ["pull", "push", "resolve"]
EOF
fi
echo "wrote ${HOSTS_DIR}/hosts.toml"

# --- Add config_path to containerd config if missing ---
CONTAINERD_CONFIG="/etc/containerd/config.toml"
if ! grep -q "config_path" "${CONTAINERD_CONFIG}"; then
    sed -i '/\[plugins\.cri\.registry\]/a\  config_path = "/etc/containerd/certs.d"' "${CONTAINERD_CONFIG}"
    echo "added config_path to ${CONTAINERD_CONFIG}"
else
    echo "config_path already present in ${CONTAINERD_CONFIG}"
fi

# --- Restart containerd ---
echo "restarting containerd..."
systemctl restart containerd
echo "done — push with: nerdctl push ${PROXY_ADDR}/<image>:<tag>"
