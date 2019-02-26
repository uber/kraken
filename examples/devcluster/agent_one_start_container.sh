#!/bin/bash

set -ex

source examples/devcluster/agent_one_param.sh

# Start kraken agent.
docker run -d \
    -p ${AGENT_PEER_PORT}:${AGENT_PEER_PORT} \
    -p ${AGENT_SERVER_PORT}:${AGENT_SERVER_PORT} \
    -p ${AGENT_REGISTRY_PORT}:${AGENT_REGISTRY_PORT} \
    -v $(pwd)/examples/devcluster/config/agent/development.yaml:/etc/kraken/config/agent/development.yaml \
    --name ${AGENT_CONTAINER_NAME} \
    kraken-agent:dev \
    /usr/bin/kraken-agent --config=/etc/kraken/config/agent/development.yaml --peer-ip=${HOSTNAME} --peer-port=${AGENT_PEER_PORT} --agent-server-port=${AGENT_SERVER_PORT} --agent-registry-port=${AGENT_REGISTRY_PORT}
