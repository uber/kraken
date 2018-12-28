#!/bin/bash

set -ex

source examples/devcluster/agent_one_param.sh

# Start kraken agent.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/agent \
    -p ${AGENT_PEER_PORT}:${AGENT_PEER_PORT} \
    -p ${AGENT_SERVER_PORT}:${AGENT_SERVER_PORT} \
    -p ${AGENT_REGISTRY_PORT}:${AGENT_REGISTRY_PORT} \
    -v $(pwd)/examples/devcluster/config/agent/development.yaml:/etc/kraken/config/agent/development.yaml \
    --name ${AGENT_CONTAINER_NAME} \
    kraken-agent:dev \
    /usr/bin/kraken-agent -config=development.yaml -peer_ip=${HOSTNAME} -peer_port=${AGENT_PEER_PORT} -agent_server_port=${AGENT_SERVER_PORT} -agent_registry_port=${AGENT_REGISTRY_PORT}
