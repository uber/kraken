#!/bin/bash

# kraken-pull.sh - Multi-host version with mandatory agent host

HERD_HOST=${HERD_HOST:-localhost:15000}

kraken_pull() {
    local image_name=$1
    local agent_host=$2
    
    # Validate required parameters
    if [ -z "$image_name" ]; then
        echo "Error: Image name is required"
        echo "Usage: kraken-pull.sh <image:tag> <agent_host:port>"
        echo "Example: kraken-pull.sh test/mysql:latest 10.0.1.101:16000"
        return 1
    fi
    
    if [ -z "$agent_host" ]; then
        echo "Error: Agent host is required"
        echo "Usage: kraken-pull.sh <image:tag> <agent_host:port>"
        echo "Example: kraken-pull.sh test/mysql:latest 10.0.1.101:16000"
        return 1
    fi
    
    echo "Pulling ${image_name} from agent: ${agent_host}"
    
    # Try the specified agent first
    if docker pull ${agent_host}/${image_name}; then
        docker tag ${agent_host}/${image_name} ${image_name}
        docker rmi ${agent_host}/${image_name}
        echo "✓ Successfully pulled via agent: ${image_name}"
        return 0
    fi
    
    # Fallback to herd if agent fails
    echo "Agent pull failed, falling back to herd: $HERD_HOST"
    if docker pull ${HERD_HOST}/${image_name}; then
        docker tag ${HERD_HOST}/${image_name} ${image_name}
        docker rmi ${HERD_HOST}/${image_name}
        echo "✓ Successfully pulled via herd: ${image_name}"
        return 0
    fi
    
    echo "✗ Failed to pull ${image_name} from both agent and herd"
    return 1
}

# Validate arguments
if [ $# -lt 2 ]; then
    echo "Error: Missing required arguments"
    echo "Usage: kraken-pull.sh <image:tag> <agent_host:port>"
    echo ""
    echo "Multi-host examples:"
    echo "  kraken-pull.sh company/app:v1.0 10.0.1.101:16000"
    echo "  kraken-pull.sh company/app:v1.0 10.0.1.102:16000"
    echo "  kraken-pull.sh company/app:v1.0 10.0.1.103:16000"
    echo ""
    echo "Environment variables:"
    echo "  HERD_HOST - Fallback herd endpoint (default: localhost:15000)"
    exit 1
fi

# Execute the pull
kraken_pull "$1" "$2"
