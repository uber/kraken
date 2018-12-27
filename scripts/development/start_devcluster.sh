#!/bin/bash

# Start testfs for test storage.
docker run -d \
    -p 15008:15008 \
    --name kraken-testfs \
    kraken-testfs:dev \
    /bin/bash -c "/usr/bin/kraken-testfs -port=15008 &>/var/log/kraken/kraken-testfs/stdout.log"

# Start origin.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/origin \
    -p 15003:15003 \
    -p 15004:15004 \
    --name kraken-origin \
    kraken-origin:dev \
    /usr/bin/kraken-origin -config=development.yaml -peer_ip=host.docker.internal -peer_port=15003 -blobserver_port=15004 -blobserver_hostname=host.docker.internal

# Start tracker.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/tracker \
    -p 15005:15005 \
    --name kraken-tracker \
    kraken-tracker:dev \
    /bin/bash -c "(redis-server --port 6380 &) && sleep 3 && /usr/bin/kraken-tracker -config=development.yaml"

# Start build index.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/build-index \
    -p 15006:15006 \
    --name kraken-build-index \
    kraken-build-index:dev \
    /usr/bin/kraken-build-index -config=development.yaml -port=15006

# Start agent.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/agent \
    -p 15000:15000 \
    -p 15001:15001 \
    -p 15002:15002 \
    --name kraken-agent \
    kraken-agent:dev \
    /usr/bin/kraken-agent -config=development.yaml -peer_ip=host.docker.internal -peer_port=15001 -agent_server_port=15002 -agent_registry_port=15000

# Start proxy.
docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/proxy \
    -p 15007:15007 \
    --name kraken-proxy \
    kraken-proxy:dev \
    /usr/bin/kraken-proxy -config=development.yaml -port=15007
