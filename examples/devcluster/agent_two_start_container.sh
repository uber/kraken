#!/bin/bash

set -ex

source examples/devcluster/agent_two_param.sh

# Start kraken agent. KRAKEN_DOCKER_EXTRA_ARGS allows callers (e.g. the perf
# benchmark harness) to inject resource constraints like --cpus or --memory.
# KRAKEN_AGENT_CONFIG_DIR overrides the host path to the agent config dir,
# defaulting to the devcluster config.
AGENT_CONFIG_DIR=${KRAKEN_AGENT_CONFIG_DIR:-$(pwd)/examples/devcluster/config/agent}
docker run -d ${KRAKEN_DOCKER_EXTRA_ARGS} \
    -p ${AGENT_PEER_PORT}:${AGENT_PEER_PORT} \
    -p ${AGENT_SERVER_PORT}:${AGENT_SERVER_PORT} \
    -p ${AGENT_REGISTRY_PORT}:${AGENT_REGISTRY_PORT} \
    -v ${AGENT_CONFIG_DIR}/development.yaml:/etc/kraken/config/agent/development.yaml \
    --name ${AGENT_CONTAINER_NAME} \
    kraken-agent:dev \
    /usr/bin/kraken-agent --config=/etc/kraken/config/agent/development.yaml --peer-ip=${HOSTNAME} --peer-port=${AGENT_PEER_PORT} --agent-server-port=${AGENT_SERVER_PORT} --agent-registry-port=${AGENT_REGISTRY_PORT} --mutex-profile-fraction=1
