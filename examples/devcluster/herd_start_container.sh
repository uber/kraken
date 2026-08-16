#!/bin/bash

set -ex

source examples/devcluster/herd_param.sh
source examples/devcluster/_devcluster_lib.sh

# Start kraken herd. KRAKEN_DOCKER_EXTRA_ARGS allows callers (e.g. the perf
# benchmark harness) to inject resource constraints like --cpus or --memory.
# KRAKEN_HERD_CONFIG_DIR overrides the host path to the per-component config
# parent dir, defaulting to the devcluster config.
HERD_CONFIG_DIR=${KRAKEN_HERD_CONFIG_DIR:-$(pwd)/examples/devcluster/config}
docker run -d ${KRAKEN_DOCKER_EXTRA_ARGS} ${HERD_NETWORK_ARGS} \
    -p ${TESTFS_PORT}:${TESTFS_PORT} \
    -p ${ORIGIN_SERVER_PORT}:${ORIGIN_SERVER_PORT} \
    -p ${ORIGIN_PEER_PORT}:${ORIGIN_PEER_PORT} \
    -p ${TRACKER_PORT}:${TRACKER_PORT} \
    -p ${BUILD_INDEX_PORT}:${BUILD_INDEX_PORT} \
    -p ${PROXY_PORT}:${PROXY_PORT} \
    -p ${PROXY_SERVER_PORT}:${PROXY_SERVER_PORT} \
    -v ${HERD_CONFIG_DIR}/origin/development.yaml:/etc/kraken/config/origin/development.yaml \
    -v ${HERD_CONFIG_DIR}/tracker/development.yaml:/etc/kraken/config/tracker/development.yaml \
    -v ${HERD_CONFIG_DIR}/build-index/development.yaml:/etc/kraken/config/build-index/development.yaml \
    -v ${HERD_CONFIG_DIR}/proxy/development.yaml:/etc/kraken/config/proxy/development.yaml \
    -v $(pwd)/examples/devcluster/herd_param.sh:/etc/kraken/herd_param.sh \
    -v $(pwd)/examples/devcluster/herd_start_processes.sh:/etc/kraken/herd_start_processes.sh \
    --name kraken-herd \
    kraken-herd:dev ./herd_start_processes.sh
