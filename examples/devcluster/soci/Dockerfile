# DinD harness for the soci-snapshotter lazy-pull e2e (Phase 2 of the streaming
# PoC). It bundles containerd, runc, CNI, nerdctl and the soci-snapshotter grpc
# plugin + CLI, and pulls images lazily (via HTTP Range) through the Kraken
# devcluster agent registry running on the host (host.docker.internal:16000).
#
# The container must be run --privileged so containerd can mount overlay/fuse.
FROM ubuntu:22.04

ARG NERDCTL_VERSION=2.0.3
ARG SOCI_VERSION=0.8.0
ARG TARGETARCH=amd64

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl iptables uidmap fuse3 \
    && rm -rf /var/lib/apt/lists/*

# nerdctl "full" bundle ships containerd, runc, CNI plugins, buildkit and
# nerdctl under /usr/local.
RUN curl -fsSL \
        "https://github.com/containerd/nerdctl/releases/download/v${NERDCTL_VERSION}/nerdctl-full-${NERDCTL_VERSION}-linux-${TARGETARCH}.tar.gz" \
    | tar -xz -C /usr/local

# soci-snapshotter grpc plugin and the soci CLI.
RUN curl -fsSL \
        "https://github.com/awslabs/soci-snapshotter/releases/download/v${SOCI_VERSION}/soci-snapshotter-${SOCI_VERSION}-linux-${TARGETARCH}.tar.gz" \
    | tar -xz -C /usr/local/bin soci soci-snapshotter-grpc

# Registry mirror hosts.toml files are generated at startup by entrypoint.sh
# (one per Kraken endpoint), so none are copied in here.
COPY config.toml /etc/containerd/config.toml
COPY soci_config.toml /etc/soci-snapshotter-grpc/config.toml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY run_e2e.sh /usr/local/bin/run_e2e.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run_e2e.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
