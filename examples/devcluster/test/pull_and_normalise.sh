#!/bin/bash

# Function to pull and normalize image name
pull_and_normalize() {
    local image_name=$1
    local agent_endpoint=$2
    local normalized_name=$3
    
    echo "Pulling ${image_name} from agent ${agent_endpoint}..."
    docker pull ${agent_endpoint}/${image_name}
    
    echo "Normalizing image name to ${normalized_name}..."
    docker tag ${agent_endpoint}/${image_name} ${normalized_name}
    
    # Optionally remove the agent-specific tag
    docker rmi ${agent_endpoint}/${image_name}
    
    echo "Image available as: ${normalized_name}"
}

# Usage examples
# pull_and_normalize "test/mysql:latest" "localhost:16000" "test/mysql:latest"
# pull_and_normalize "test/mysql:latest" "localhost:17000" "test/mysql:latest"
