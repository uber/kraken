#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/origin \
    -p 5003:5003 \
    -p 5004:5004 \
    kraken-origin:dev \
    /usr/bin/kraken-origin -config=development.yaml -peer_ip=host.docker.internal -peer_port=5003 -blobserver_port=5004 -blobserver_hostname=host.docker.internal
