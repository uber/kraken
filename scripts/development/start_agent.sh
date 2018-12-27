#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/agent \
    -p 15000:15000 \
    -p 15001:15001 \
    -p 15002:15002 \
    --name kraken-agent \
    kraken-agent:dev \
    /usr/bin/kraken-agent -config=development.yaml -peer_ip=host.docker.internal -peer_port=15001 -agent_server_port=15002 -agent_registry_port=15000
