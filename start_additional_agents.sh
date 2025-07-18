#!/bin/bash

# Script to start additional Kraken agents for P2P testing
# Usage: ./start_additional_agents.sh

set -e

echo "Starting additional Kraken agents..."

# Start agent three
echo "Starting kraken-agent-three..."
./examples/devcluster/agent_three_start_container.sh

# Start agent four  
echo "Starting kraken-agent-four..."
./examples/devcluster/agent_four_start_container.sh

# Start agent five
echo "Starting kraken-agent-five..."
./examples/devcluster/agent_five_start_container.sh

echo "All additional agents started successfully!"
echo ""
echo "Agent endpoints:"
echo "- Agent Three: localhost:18000"
echo "- Agent Four:  localhost:19000" 
echo "- Agent Five:  localhost:20000"
echo ""
echo "To test P2P distribution, try:"
echo "docker pull localhost:18000/test/hello-world:latest"
echo "docker pull localhost:19000/test/hello-world:latest"
echo "docker pull localhost:20000/test/hello-world:latest"
