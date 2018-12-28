#!/bin/bash

# Start kraken herd.
docker run -d \
    -p 15003:15003 \
    -p 15004:15004 \
    -p 15005:15005 \
    -p 15006:15006 \
    -p 15007:15007 \
    -p 15008:15008 \
    --name kraken-herd \
    kraken-herd:dev

# Start kraken agent.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/agent \
    -p 15000:15000 \
    -p 15001:15001 \
    -p 15002:15002 \
    --name kraken-agent \
    kraken-agent:dev \
    /usr/bin/kraken-agent -config=development.yaml -peer_ip=host.docker.internal -peer_port=15001 -agent_server_port=15002 -agent_registry_port=15000
