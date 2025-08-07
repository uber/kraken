#!/bin/bash

# Multi-host Kraken Herd deployment script

HERD_HOST_IP=${1:-""}

if [ -z "$HERD_HOST_IP" ]; then
    echo "Usage: ./deploy_herd.sh <herd_host_ip>"
    echo "Example: ./deploy_herd.sh 10.0.1.100"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MULTIHOST_DIR="$(dirname "$SCRIPT_DIR")"

export HERD_HOST_IP

echo "=== Deploying Kraken Herd on ${HERD_HOST_IP} ==="

# Build images if needed
echo "Building Kraken images..."
(cd "$MULTIHOST_DIR/../../" && make images)

# Start herd
echo "Starting herd container..."
(cd "$MULTIHOST_DIR" && HERD_HOST_IP="$HERD_HOST_IP" ./herd_start_container.sh)

echo ""
echo "=== Herd Deployment Complete! ==="
echo ""
echo "Services available at:"
echo "  - Push Images: docker push ${HERD_HOST_IP}:15000/<image>:<tag>"
echo "  - Tracker: ${HERD_HOST_IP}:15003"
echo "  - TestFS: ${HERD_HOST_IP}:14000"
echo ""
echo "Next: Deploy agents on other hosts using:"
echo "  ./deploy_agent.sh ${HERD_HOST_IP} <agent_host_ip>"
