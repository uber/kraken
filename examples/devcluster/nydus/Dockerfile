# DinD harness for the nydus-snapshotter lazy-pull e2e benchmark, run over the
# SAME eStargz images as the estargz harness. nydus-snapshotter can lazily mount
# eStargz layers directly (no nydus-native RAFS conversion) via its experimental
# enable_stargz mode: it downloads each layer's eStargz TOC, builds a small nydus
# bootstrap with `nydus-image --source-type stargz_index`, and serves reads on
# demand through nydusd's registry backend (HTTP Range) against the Kraken agents.
#
# This proves Kraken's range read path is not just format-agnostic (soci/estargz)
# but snapshotter-agnostic: a third snapshotter streams the same blobs with no
# Kraken change.
#
# Binaries: nerdctl-full gives containerd/runc/CNI/nerdctl (its bundled stargz
# plugin is unused here). nydus is NOT in nerdctl-full, so it is downloaded
# separately: containerd-nydus-grpc (the snapshotter) from nydus-snapshotter, and
# nydusd + nydus-image (runtime daemon + the per-layer bootstrap builder the
# eStargz mount path invokes) from dragonflyoss/nydus.
#
# The container must be run --privileged so containerd/nydusd can mount fuse.
FROM ubuntu:22.04

ARG NERDCTL_VERSION=2.0.3
ARG NYDUS_SNAPSHOTTER_VERSION=0.15.15
# nydus 2.1.6, NOT 2.4.x: the eStargz lazy path in nydus-snapshotter v0.15.15
# (pkg/filesystem/stargz_adaptor.go) shells out to `nydus-image create` with
# flags that nydus removed/renamed after the 2.1 line -- `--source-type`,
# `--disable-check`, and `--blob-meta <path>` (a SEPARATE meta file nydusd then
# reads). 2.2+ renamed these to `--type` and `--blob-inline-meta` (inlined, no
# file), so the bootstrap build fails "exit status 2" per layer and nydus
# silently falls back to a full containerd download. 2.1.6 is the last release
# whose CLI matches the snapshotter's hard-coded flags. Verified: v2.1.6
# nydus-image create accepts source-type=stargz_index, blob-meta, disable-check,
# fs-version, chunk-size.
ARG NYDUS_VERSION=2.1.6
ARG TARGETARCH=amd64

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl iptables uidmap fuse3 \
    && rm -rf /var/lib/apt/lists/*

# nerdctl "full" bundle: containerd, runc, CNI plugins, buildkit, nerdctl.
RUN curl -fsSL \
        "https://github.com/containerd/nerdctl/releases/download/v${NERDCTL_VERSION}/nerdctl-full-${NERDCTL_VERSION}-linux-${TARGETARCH}.tar.gz" \
    | tar -xz -C /usr/local

# nydus-snapshotter: the containerd-nydus-grpc proxy snapshotter plugin.
RUN curl -fsSL \
        "https://github.com/containerd/nydus-snapshotter/releases/download/v${NYDUS_SNAPSHOTTER_VERSION}/nydus-snapshotter-v${NYDUS_SNAPSHOTTER_VERSION}-linux-${TARGETARCH}.tar.gz" \
        -o /tmp/ns.tgz \
    && tar -xzf /tmp/ns.tgz -C /tmp \
    && cp /tmp/bin/containerd-nydus-grpc /tmp/bin/nydus-overlayfs /usr/local/bin/ \
    && rm -rf /tmp/ns.tgz /tmp/bin

# dragonflyoss/nydus: nydusd (the user-space daemon doing on-demand Range fetch)
# and nydus-image (the eStargz mount path builds a per-layer bootstrap with it).
RUN curl -fsSL \
        "https://github.com/dragonflyoss/nydus/releases/download/v${NYDUS_VERSION}/nydus-static-v${NYDUS_VERSION}-linux-${TARGETARCH}.tgz" \
        -o /tmp/nydus.tgz \
    && tar -xzf /tmp/nydus.tgz -C /tmp \
    && cp /tmp/nydus-static/nydusd /tmp/nydus-static/nydus-image /usr/local/bin/ \
    && rm -rf /tmp/nydus.tgz /tmp/nydus-static

# Registry mirror hosts.toml files are generated at startup by entrypoint.sh
# (one per Kraken endpoint), so none are copied in here.
COPY config.toml /etc/containerd/config.toml
COPY nydus_snapshotter.toml /etc/nydus/config.toml
COPY nydusd-config.fusedev.json /etc/nydus/nydusd-config.fusedev.json
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY run_e2e.sh /usr/local/bin/run_e2e.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run_e2e.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
