#!/bin/bash

set -eu

systemctl stop subd
curl -Lo /tmp/nerdctl.tar.gz https://github.com/containerd/nerdctl/releases/download/v2.0.4/nerdctl-2.0.4-linux-amd64.tar.gz
tar xf /tmp/nerdctl.tar.gz -C /usr/local/bin/ nerdctl

mkdir /tmp/apt-recovery && cd /tmp/apt-recovery

APT_FILE=$(wget -qO- http://ftp.debian.org/debian/pool/main/a/apt/ | grep -o 'apt_2.6[^"]*_amd64\.deb' | tail -n 1)
wget "http://ftp.debian.org/debian/pool/main/a/apt/$APT_FILE"

dpkg -i "$APT_FILE"

KEYRING_FILE=$(wget -qO- http://ftp.debian.org/debian/pool/main/d/debian-archive-keyring/ | grep -o 'debian-archive-keyring_[^"]*_all\.deb' | tail -n 1)
wget "http://ftp.debian.org/debian/pool/main/d/debian-archive-keyring/$KEYRING_FILE"

dpkg -i "$KEYRING_FILE"

dpkg --configure -a

cat <<EOF > /etc/apt/sources.list
deb http://deb.debian.org/debian bookworm main
deb http://deb.debian.org/debian-security bookworm-security main
deb http://deb.debian.org/debian bookworm-updates main
EOF

apt-get update && apt-get install fuse

tar -C /usr/local/bin -xvf stargz-snapshotter-v0.18.2-linux-amd64.tar.gz containerd-stargz-grpc ctr-remote

# Edit containerd config also

wget -O /etc/systemd/system/stargz-snapshotter.service https://raw.githubusercontent.com/containerd/stargz-snapshotter/main/script/config/etc/systemd/system/stargz-snapshotter.service
systemctl enable --now stargz-snapshotter
systemctl restart containerd