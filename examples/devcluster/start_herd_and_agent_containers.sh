#!/bin/bash

set -ex

source examples/devcluster/param.sh

# Start kraken herd.
docker run -d \
    -p ${TESTFS_PORT}:${TESTFS_PORT} \
    -p ${ORIGIN_SERVER_PORT}:${ORIGIN_SERVER_PORT} \
    -p ${ORIGIN_PEER_PORT}:${ORIGIN_PEER_PORT} \
    -p ${TRACKER_PORT}:${TRACKER_PORT} \
    -p ${BUILD_INDEX_PORT}:${BUILD_INDEX_PORT} \
    -p ${PROXY_PORT}:${PROXY_PORT} \
    -v $(pwd)/examples/devcluster/config/origin/development.yaml:/etc/kraken/config/origin/development.yaml \
    -v $(pwd)/examples/devcluster/config/tracker/development.yaml:/etc/kraken/config/tracker/development.yaml \
    -v $(pwd)/examples/devcluster/config/build-index/development.yaml:/etc/kraken/config/build-index/development.yaml \
    -v $(pwd)/examples/devcluster/config/proxy/development.yaml:/etc/kraken/config/proxy/development.yaml \
    -v $(pwd)/examples/devcluster/param.sh:/etc/kraken/param.sh \
    -v $(pwd)/examples/devcluster/start_herd_processes.sh:/etc/kraken/start_herd_processes.sh \
    --name kraken-herd \
    kraken-herd:dev ./start_herd_processes.sh

# Start kraken agent.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/agent \
    -p ${AGENT_PEER_PORT}:${AGENT_PEER_PORT} \
    -p ${AGENT_SERVER_PORT}:${AGENT_SERVER_PORT} \
    -p ${AGENT_REGISTRY_PORT}:${AGENT_REGISTRY_PORT} \
    -v $(pwd)/examples/devcluster/config/agent/development.yaml:/etc/kraken/config/agent/development.yaml \
    --name kraken-agent \
    kraken-agent:dev \
    /usr/bin/kraken-agent -config=development.yaml -peer_ip=host.docker.internal -peer_port=${AGENT_PEER_PORT} -agent_server_port=${AGENT_SERVER_PORT} -agent_registry_port=${AGENT_REGISTRY_PORT}
