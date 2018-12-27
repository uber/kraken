#!/bin/bash

docker run -d \
    -e UBER_CONFIG_DIR=/etc/kraken/config/tracker \
    -p 15005:15005 \
    --name kraken-tracker \
    kraken-tracker:dev \
    /bin/bash -c "(redis-server --port 6380 &) && sleep 3 && /usr/bin/kraken-tracker -config=development.yaml"
