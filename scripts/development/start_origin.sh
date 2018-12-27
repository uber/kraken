#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/origin \
    -p 15003:15003 \
    -p 15004:15004 \
    --name kraken-origin \
    kraken-origin:dev \
    /usr/bin/kraken-origin -config=development.yaml -peer_ip=host.docker.internal -peer_port=15003 -blobserver_port=15004 -blobserver_hostname=host.docker.internal
