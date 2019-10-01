#!/bin/bash

source /etc/kraken/herd_param.sh

redis-server --port ${REDIS_PORT} &

sleep 3

/usr/bin/kraken-testfs \
    --port=${TESTFS_PORT} \
    &>/var/log/kraken/kraken-testfs/stdout.log &

/usr/bin/kraken-origin \
    --config=/etc/kraken/config/origin/development.yaml \
    --blobserver-hostname=${HOSTNAME} \
    --blobserver-port=${ORIGIN_SERVER_PORT} \
    --peer-ip=${HOSTNAME} \
    --peer-port=${ORIGIN_PEER_PORT} \
    &>/var/log/kraken/kraken-origin/stdout.log &

/usr/bin/kraken-tracker \
    --config=/etc/kraken/config/tracker/development.yaml \
    --port=${TRACKER_PORT} \
    &>/var/log/kraken/kraken-tracker/stdout.log &

/usr/bin/kraken-build-index \
    --config=/etc/kraken/config/build-index/development.yaml \
    --port=${BUILD_INDEX_PORT} \
    &>/var/log/kraken/kraken-build-index/stdout.log &

/usr/bin/kraken-proxy \
    --config=/etc/kraken/config/proxy/development.yaml \
    --port=${PROXY_PORT} \
    --server-port=${PROXY_SERVER_PORT} \
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
