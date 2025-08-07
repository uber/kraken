#!/bin/bash

set -ex

# Validate required environment variables
if [ -z "$HERD_HOST_IP" ]; then
    echo "Error: HERD_HOST_IP environment variable is required"
    echo "Usage: HERD_HOST_IP=10.0.1.100 AGENT_HOST_IP=10.0.1.101 ./agent_start_container.sh"
    exit 1
fi

if [ -z "$AGENT_HOST_IP" ]; then
    echo "Error: AGENT_HOST_IP environment variable is required"
    echo "Usage: HERD_HOST_IP=10.0.1.100 AGENT_HOST_IP=10.0.1.101 ./agent_start_container.sh"
    exit 1
fi

source agent_param.sh

echo "Starting Kraken Agent on ${AGENT_HOST_IP}..."
echo "Connecting to Herd at ${HERD_HOST_IP}..."

# Start kraken agent with multi-host configuration
docker run -d \
    -p ${AGENT_PEER_PORT}:${AGENT_PEER_PORT} \
    -p ${AGENT_SERVER_PORT}:${AGENT_SERVER_PORT} \
    -p ${AGENT_REGISTRY_PORT}:${AGENT_REGISTRY_PORT} \
    -e HERD_HOST_IP=${HERD_HOST_IP} \
    -e AGENT_HOST_IP=${AGENT_HOST_IP} \
    -v $(pwd)/config/agent/multihost.yaml:/etc/kraken/config/agent/multihost.yaml \
    --name ${AGENT_CONTAINER_NAME}-$(hostname) \
    kraken-agent:dev \
    /usr/bin/kraken-agent \
    --config=/etc/kraken/config/agent/multihost.yaml \
    --peer-ip=${AGENT_HOST_IP} \
    --peer-port=${AGENT_PEER_PORT} \
    --agent-server-port=${AGENT_SERVER_PORT} \
    --agent-registry-port=${AGENT_REGISTRY_PORT}

echo "Kraken Agent started successfully!"
echo "Agent available at: ${AGENT_HOST_IP}:16000"
