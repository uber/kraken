#!/bin/bash

source /etc/kraken/herd_param.sh

redis-server --port ${REDIS_PORT} --bind ${BIND_ADDRESS} &

sleep 3

/usr/bin/kraken-testfs \
    --port=${TESTFS_PORT} \
    &>/var/log/kraken/kraken-testfs/stdout.log &

# Substitute environment variables in config files
envsubst < /etc/kraken/config/origin/multihost.yaml > /tmp/origin.yaml
envsubst < /etc/kraken/config/tracker/multihost.yaml > /tmp/tracker.yaml
envsubst < /etc/kraken/config/build-index/multihost.yaml > /tmp/build-index.yaml
envsubst < /etc/kraken/config/proxy/multihost.yaml > /tmp/proxy.yaml

/usr/bin/kraken-origin \
    --config=/tmp/origin.yaml \
    --blobserver-hostname=${HOSTNAME} \
    --blobserver-port=${ORIGIN_SERVER_PORT} \
    --peer-ip=${HERD_HOST_IP} \
    --peer-port=${ORIGIN_PEER_PORT} \
    &>/var/log/kraken/kraken-origin/stdout.log &

/usr/bin/kraken-tracker \
    --config=/tmp/tracker.yaml \
    --port=${TRACKER_PORT} \
    &>/var/log/kraken/kraken-tracker/stdout.log &

/usr/bin/kraken-build-index \
    --config=/tmp/build-index.yaml \
    --port=${BUILD_INDEX_PORT} \
    &>/var/log/kraken/kraken-build-index/stdout.log &

/usr/bin/kraken-proxy \
    --config=/tmp/proxy.yaml \
    --port=${PROXY_PORT} \
    &>/var/log/kraken/kraken-proxy/stdout.log &

sleep 3

echo "Kraken Herd started on ${HOSTNAME} (${HERD_HOST_IP})"
echo "Services:"
echo "  - Proxy: ${HERD_HOST_IP}:${PROXY_PORT}"
echo "  - Tracker: ${HERD_HOST_IP}:${TRACKER_PORT}"
echo "  - TestFS: ${HERD_HOST_IP}:${TESTFS_PORT}"

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
