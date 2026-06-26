#!/bin/bash

# Parameterized agent start script for the perf benchmark harness. Takes the
# agent index as its first argument and computes ports as
# 15000 + INDEX*1000 + offset, matching the convention used by the
# agent_one/agent_two scripts (index 1 -> 16xxx, index 2 -> 17xxx, etc.).
# Honors KRAKEN_DOCKER_EXTRA_ARGS and KRAKEN_AGENT_CONFIG_DIR like the other
# devcluster scripts.

set -ex

AGENT_INDEX=${1:?usage: $0 <agent-index>}

source examples/devcluster/_devcluster_lib.sh

AGENT_REGISTRY_PORT=$((15000 + AGENT_INDEX * 1000))
AGENT_PEER_PORT=$((AGENT_REGISTRY_PORT + 1))
AGENT_SERVER_PORT=$((AGENT_REGISTRY_PORT + 2))

HOSTNAME=host.docker.internal
AGENT_CONTAINER_NAME=kraken-agent-${AGENT_INDEX}

AGENT_CONFIG_DIR=${KRAKEN_AGENT_CONFIG_DIR:-$(pwd)/examples/devcluster/config/agent}

docker run -d ${KRAKEN_DOCKER_EXTRA_ARGS} ${NETWORK_ARGS} \
    -p ${AGENT_PEER_PORT}:${AGENT_PEER_PORT} \
    -p ${AGENT_SERVER_PORT}:${AGENT_SERVER_PORT} \
    -p ${AGENT_REGISTRY_PORT}:${AGENT_REGISTRY_PORT} \
    -v ${AGENT_CONFIG_DIR}/development.yaml:/etc/kraken/config/agent/development.yaml \
    --name ${AGENT_CONTAINER_NAME} \
    kraken-agent:dev \
    /usr/bin/kraken-agent \
        --config=/etc/kraken/config/agent/development.yaml \
        --peer-ip=${HOSTNAME} \
        --peer-port=${AGENT_PEER_PORT} \
        --agent-server-port=${AGENT_SERVER_PORT} \
        --agent-registry-port=${AGENT_REGISTRY_PORT} \
        --mutex-profile-fraction=1
