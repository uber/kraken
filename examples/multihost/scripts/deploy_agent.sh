#!/bin/bash

# Multi-host Kraken Agent deployment script

HERD_HOST_IP=${1:-""}
AGENT_HOST_IP=${2:-""}

if [ -z "$HERD_HOST_IP" ] || [ -z "$AGENT_HOST_IP" ]; then
    echo "Usage: ./deploy_agent.sh <herd_host_ip> <agent_host_ip>"
    echo "Example: ./deploy_agent.sh 10.0.1.100 10.0.1.101"
    exit 1
fi

export HERD_HOST_IP
export AGENT_HOST_IP

echo "=== Deploying Kraken Agent on ${AGENT_HOST_IP} ==="
echo "Connecting to Herd at ${HERD_HOST_IP}"

# Ensure agent image exists (copy from herd host or build locally)
if ! docker image inspect kraken-agent:dev >/dev/null 2>&1; then
    echo "Agent image not found. Please build on herd host and copy, or build locally:"
    echo "  make images"
    exit 1
fi

# Start agent
echo "Starting agent container..."
./examples/multihost/agent_start_container.sh

echo ""
echo "=== Agent Deployment Complete! ==="
echo ""
echo "Agent available at: ${AGENT_HOST_IP}:16000"
echo "Pull images: docker pull ${AGENT_HOST_IP}:16000/<image>:<tag>"
