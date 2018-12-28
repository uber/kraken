#!/bin/bash

baseconfig=/etc/kraken/config
hostname=host.docker.internal

redis-server --port 6380 &

sleep 3

/usr/bin/kraken-testfs \
    -port=15008 \
    &>/var/log/kraken/kraken-testfs/stdout.log &

UBER_CONFIG_DIR=$baseconfig/origin /usr/bin/kraken-origin \
    -blobserver_hostname=$hostname \
    -blobserver_port=15004 \
    -peer_ip=$hostname \
    -peer_port=15003 \
    -config=development.yaml \
    &>/var/log/kraken/kraken-origin/stdout.log &

UBER_CONFIG_DIR=$baseconfig/tracker /usr/bin/kraken-tracker \
    -config=development.yaml \
    &>/var/log/kraken/kraken-tracker/stdout.log &

UBER_CONFIG_DIR=$baseconfig/build-index /usr/bin/kraken-build-index \
    -config=development.yaml \
    -port=15006 \
    &>/var/log/kraken/kraken-build-index/stdout.log &

UBER_CONFIG_DIR=$baseconfig/proxy /usr/bin/kraken-proxy \
    -config=development.yaml \
    -port=15007 \
    &>/var/log/kraken/kraken-proxy/stdout.log &

sleep 3

# Poor man's supervisor.
while : ; do
    for c in redis-server kraken-testfs kraken-origin kraken-tracker kraken-build-index kraken-proxy; do
        ps aux | grep $c | grep -q -v grep
        status=$?
        if [ $status -ne 0 ]; then
            echo "$c exited unexpectedly. Logs:"
            tail -100 /var/log/kraken/$c/stdout.log
            exit 1
        fi
    done
    sleep 30
done
