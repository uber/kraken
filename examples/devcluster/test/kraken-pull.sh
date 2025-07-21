#!/bin/bash

# kraken-pull.sh - Wrapper script for consistent image naming

HERD_HOST=${HERD_HOST:-localhost:15000}
AGENT_HOSTS=(
    "localhost:16000"
    "localhost:17000" 
    "localhost:18000"
)

kraken_pull() {
    local image_name=$1
    local prefer_agent=${2:-""}
    
    # If preferred agent specified, try that first
    if [ ! -z "$prefer_agent" ]; then
        echo "Attempting pull from preferred agent: $prefer_agent"
        if docker pull ${prefer_agent}/${image_name}; then
            docker tag ${prefer_agent}/${image_name} ${image_name}
            docker rmi ${prefer_agent}/${image_name}
            echo "Successfully pulled: ${image_name}"
            return 0
        fi
    fi
    
    # Try each agent in order
    for agent in "${AGENT_HOSTS[@]}"; do
        echo "Attempting pull from agent: $agent"
        if docker pull ${agent}/${image_name}; then
            docker tag ${agent}/${image_name} ${image_name}
            docker rmi ${agent}/${image_name}
            echo "Successfully pulled: ${image_name}"
            return 0
        fi
    done
    
    # Fallback to herd
    echo "Pulling from herd: $HERD_HOST"
    docker pull ${HERD_HOST}/${image_name}
    docker tag ${HERD_HOST}/${image_name} ${image_name}
    docker rmi ${HERD_HOST}/${image_name}
    echo "Successfully pulled: ${image_name}"
}

# Usage: kraken-pull.sh image:tag [preferred_agent]
kraken_pull "$1" "$2"
