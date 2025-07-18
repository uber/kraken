#!/bin/bash

# Script to stop additional Kraken agents
# Usage: ./stop_additional_agents.sh

set -e

echo "Stopping additional Kraken agents..."

# Stop and remove additional agent containers
for agent in kraken-agent-three kraken-agent-four kraken-agent-five; do
    if docker ps -a --format '{{.Names}}' | grep -q "^${agent}$"; then
        echo "Stopping and removing $agent..."
        docker rm -f $agent
    else
        echo "$agent not found"
    fi
done

echo "All additional agents stopped!"
