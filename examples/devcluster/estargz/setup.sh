#!/bin/bash

set -eu

systemctl stop subd
curl -Lo /tmp/nerdctl.tar.gz https://github.com/containerd/nerdctl/releases/download/v2.0.4/nerdctl-2.0.4-linux-amd64.tar.gz
tar xf /tmp/nerdctl.tar.gz -C /usr/local/bin/ nerdctl

mkdir /tmp/apt-recovery && cd /tmp/apt-recovery

APT_FILE=$(wget -qO- http://ftp.debian.org/debian/pool/main/a/apt/ | grep -o 'apt_2.6[^"]*_amd64\.deb' | tail -n 1)
wget "http://ftp.debian.org/debian/pool/main/a/apt/$APT_FILE"

dpkg -i "$APT_FILE" || true

KEYRING_FILE=$(wget -qO- http://ftp.debian.org/debian/pool/main/d/debian-archive-keyring/ | grep -o 'debian-archive-keyring_[^"]*_all\.deb' | tail -n 1)
wget "http://ftp.debian.org/debian/pool/main/d/debian-archive-keyring/$KEYRING_FILE"

dpkg -i "$KEYRING_FILE"

dpkg --configure -a

cat <<EOF > /etc/apt/sources.list
deb http://deb.debian.org/debian bookworm main
deb http://deb.debian.org/debian-security bookworm-security main
deb http://deb.debian.org/debian bookworm-updates main
EOF

apt-get update && apt-get install -y fuse

curl -Lo stargz-snapshotter-v0.18.2-linux-amd64.tar.gz https://github.com/containerd/stargz-snapshotter/releases/download/v0.18.2/stargz-snapshotter-v0.18.2-linux-amd64.tar.gz
tar -C /usr/local/bin -xvf stargz-snapshotter-v0.18.2-linux-amd64.tar.gz containerd-stargz-grpc ctr-remote

# Register the stargz snapshotter proxy plugin with containerd (idempotent).
# Does NOT change plugins.cri.containerd.snapshotter -- that stays "overlayfs"
# so the baseline leg is unaffected; nerdctl selects --snapshotter=stargz
# per-pull for the lazy leg.
if ! grep -q '\[proxy_plugins.stargz\]' /etc/containerd/config.toml; then
  cp /etc/containerd/config.toml /etc/containerd/config.toml.bak
  cat <<EOF >> /etc/containerd/config.toml

[proxy_plugins]
  [proxy_plugins.stargz]
    type = "snapshot"
    address = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"
  [proxy_plugins.stargz.exports]
    root = "/var/lib/containerd-stargz-grpc/"
EOF
fi

# containerd-stargz-grpc's own config -- points it at the same Kraken
# registry mirror already configured in containerd's
# plugins.cri.registry.mirrors."127.0.0.1:5055" above.
#
# [blob] controls retries for actual chunk/blob fetch requests (the lazy-read
# data path) -- fs/config/config.go's BlobConfig: max_retries (default 5),
# min_wait_msec/max_wait_msec (backoff bounds, default 30/30).
#
# [resolver] controls the manifest/blob-descriptor resolve requests against
# the mirror host -- service/resolver/registry.go's Config. Its HTTP client
# retries automatically via hashicorp/go-retryablehttp regardless of config;
# request_timeout_sec is the only tunable there.
mkdir -p /etc/containerd-stargz-grpc
cat <<EOF > /etc/containerd-stargz-grpc/config.toml
debug = true
noprefetch = false
no_background_fetch = false
max_concurrency = 1
prefetch_timeout_sec = 300

[resolver]
  request_timeout_sec = 300

  [[resolver.host."127.0.0.1:5055".mirrors]]
    host = "127.0.0.1:5055"
    insecure = true
    request_timeout_sec = 300

[blob]
max_retries = 10
min_wait_msec = 1000
max_wait_msec = 15000
fetching_timeout_sec = 600
check_always = true
force_single_range_mode = true
EOF

wget -O /etc/systemd/system/stargz-snapshotter.service https://raw.githubusercontent.com/containerd/stargz-snapshotter/main/script/config/etc/systemd/system/stargz-snapshotter.service
systemctl enable --now stargz-snapshotter
systemctl restart containerd