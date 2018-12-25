#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/build-index \
    -p 5006:5006 \
    kraken-build-index:dev \
    /usr/bin/kraken-build-index -config=development.yaml -port=5006
