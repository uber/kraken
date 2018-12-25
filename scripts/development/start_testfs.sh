#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/build-index \
    -p 5008:5008 \
    kraken-build-index:dev \
    /usr/bin/kraken-build-index -config=development.yaml -port=5008
