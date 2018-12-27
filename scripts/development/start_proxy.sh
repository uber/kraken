#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/proxy \
    -p 15007:15007 \
    --name kraken-proxy \
    kraken-proxy:dev \
    /usr/bin/kraken-proxy -config=development.yaml -port=15007
