#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/agent \
    -p 5001:5001 \
    -p 5002:5002 \
    -p 5555:5555 \
    kraken-agent:dev \
    /usr/bin/kraken-agent -config=development.yaml -peer_ip=host.docker.internal -peer_port=5001 -agent_server_port=5002 -agent_registry_port=5555
