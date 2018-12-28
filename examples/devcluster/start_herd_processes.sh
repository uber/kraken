#!/bin/bash

source /etc/kraken/param.sh

CONFIG_DIR=/etc/kraken/config

redis-server --port ${REDIS_PORT} &

sleep 3

/usr/bin/kraken-testfs \
    -port=${TESTFS_PORT} \
    &>/var/log/kraken/kraken-testfs/stdout.log &

UBER_CONFIG_DIR=${CONFIG_DIR}/origin /usr/bin/kraken-origin \
    -blobserver_hostname=${HOSTNAME} \
    -blobserver_port=${ORIGIN_SERVER_PORT} \
    -peer_ip=${HOSTNAME} \
    -peer_port=${ORIGIN_PEER_PORT} \
    -config=development.yaml \
    &>/var/log/kraken/kraken-origin/stdout.log &

UBER_CONFIG_DIR=${CONFIG_DIR}/tracker /usr/bin/kraken-tracker \
    -config=development.yaml \
    &>/var/log/kraken/kraken-tracker/stdout.log &

UBER_CONFIG_DIR=${CONFIG_DIR}/build-index /usr/bin/kraken-build-index \
    -config=development.yaml \
    -port=${BUILD_INDEX_PORT} \
    &>/var/log/kraken/kraken-build-index/stdout.log &

UBER_CONFIG_DIR=${CONFIG_DIR}/proxy /usr/bin/kraken-proxy \
    -config=development.yaml \
    -port=${PROXY_PORT} \
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
