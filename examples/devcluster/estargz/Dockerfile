# DinD harness for the stargz-snapshotter (eStargz) lazy-pull e2e benchmark, the
# format-agnostic counterpart to the soci harness. It bundles containerd, runc,
# CNI, nerdctl and -- via the nerdctl "full" bundle -- the containerd-stargz-grpc
# plugin + ctr-remote, and pulls images lazily (HTTP Range) through the Kraken
# devcluster agent registries running on the host.
#
# Unlike the soci harness, no separate snapshotter download is needed: the
# nerdctl-full bundle already ships containerd-stargz-grpc and ctr-remote (stargz
# v0.16.3 in nerdctl-full v2.0.3).
#
# The container must be run --privileged so containerd can mount overlay/fuse.
FROM ubuntu:22.04

ARG NERDCTL_VERSION=2.0.3
ARG TARGETARCH=amd64

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl iptables uidmap fuse3 \
    && rm -rf /var/lib/apt/lists/*

# nerdctl "full" bundle ships containerd, runc, CNI plugins, buildkit, nerdctl
# AND the stargz snapshotter (containerd-stargz-grpc + ctr-remote) under
# /usr/local.
RUN curl -fsSL \
        "https://github.com/containerd/nerdctl/releases/download/v${NERDCTL_VERSION}/nerdctl-full-${NERDCTL_VERSION}-linux-${TARGETARCH}.tar.gz" \
    | tar -xz -C /usr/local

# Registry mirror hosts.toml files are generated at startup by entrypoint.sh
# (one per Kraken endpoint), so none are copied in here.
COPY config.toml /etc/containerd/config.toml
COPY stargz_config.toml /etc/containerd-stargz-grpc/config.toml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY run_e2e.sh /usr/local/bin/run_e2e.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run_e2e.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
