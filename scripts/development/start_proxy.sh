#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/build-index \
    -p 5007:5007 \
    kraken-build-index:dev \
    /usr/bin/kraken-build-index -config=development.yaml -port=5007
