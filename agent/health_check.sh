#!/bin/bash

set -vx

while true; do
    sleep 30s
    date
    curl --connect-timeout 5 localhost:7602/health
    if [ $? -ne 0 ]; then
        m3send --service=kraken-agent --env=$KRAKEN_CLUSTER --tag=hostname:$(hostname) --type=counter --name=unhealthy --val=1
    fi
done
