#!/bin/bash

HERD_HOST=${1:-"localhost"}
AGENT_HOSTS=${2:-"localhost"}

echo "=== Testing Multi-Host Kraken Deployment ==="
echo "Herd: ${HERD_HOST}:15000"
echo "Agent: ${AGENT_HOSTS}:16000"

# Step 1: Push test image to herd
echo "1. Pushing test image to herd..."
docker pull hello-world
docker tag hello-world ${HERD_HOST}:15000/test/hello-world:latest
docker push ${HERD_HOST}:15000/test/hello-world:latest

echo "2. Waiting for distribution..."
sleep 10

# Step 2: Pull from agent
echo "3. Pulling from agent (should use P2P)..."
./examples/multihost/test/kraken-pull.sh test/hello-world:latest ${AGENT_HOSTS}:16000

echo "4. Verifying image..."
docker images | grep hello-world

echo ""
echo "=== Multi-Host Test Complete! ==="
echo ""
echo "Workflow tested:"
echo "1. Push to Herd: ${HERD_HOST}:15000/test/hello-world:latest"
echo "2. Pull from Agent: ${AGENT_HOSTS}:16000/test/hello-world:latest"
echo "3. Image normalized to: test/hello-world:latest"
