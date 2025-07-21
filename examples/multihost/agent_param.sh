#!/bin/bash

# Define agent ports (same port on all hosts).
AGENT_REGISTRY_PORT=16000
AGENT_PEER_PORT=16001
AGENT_SERVER_PORT=16002

# Multi-host networking
HERD_HOST_IP=${HERD_HOST_IP:-localhost}
AGENT_HOST_IP=${AGENT_HOST_IP:-localhost}

# Container config.
AGENT_CONTAINER_NAME=kraken-agent
AGENT_ID=${AGENT_ID:-agent-$(hostname)}

# Allow external access
BIND_ADDRESS="0.0.0.0"
