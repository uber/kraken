#!/bin/bash

docker run -d \
    -p 15008:15008 \
    --name kraken-testfs \
    kraken-testfs:dev \
    /bin/bash -c "/usr/bin/kraken-testfs -port=15008 &>/var/log/kraken/kraken-testfs/stdout.log"
