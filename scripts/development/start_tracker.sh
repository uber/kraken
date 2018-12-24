#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/tracker \
    -p 5005:5005 \
    kraken-tracker:dev \
    /usr/bin/kraken-tracker -config=development.yaml
