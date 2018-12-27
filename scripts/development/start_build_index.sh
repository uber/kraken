#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/build-index \
    -p 15006:15006 \
    --name kraken-build-index \
    kraken-build-index:dev \
    /usr/bin/kraken-build-index -config=development.yaml -port=15006
