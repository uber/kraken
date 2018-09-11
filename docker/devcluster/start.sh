#!/bin/bash

baseconfig=/home/udocker/kraken/config

redis-server &

sleep 3

/usr/bin/kraken-testfs \
    -port=7357 \
    &>/var/log/udocker/kraken-testfs/stdout.log &

UBER_CONFIG_DIR=$baseconfig/origin /usr/bin/kraken-origin \
    -blobserver_port=9003 \
    -peer_ip=localhost \
    -peer_port=5081 \
    -config=devcluster.yaml \
    -zone=devcluster \
    -cluster=devcluster \
    &>/var/log/udocker/kraken-origin/stdout.log &

UBER_CONFIG_DIR=$baseconfig/build-index /usr/bin/kraken-build-index \
    -config=devcluster.yaml \
    -cluster=devcluster \
    -port=5263 \
    &>/var/log/udocker/kraken-build-index/stdout.log &

UBER_CONFIG_DIR=$baseconfig/tracker /usr/bin/kraken-tracker \
    -config=devcluster.yaml \
    -cluster=devcluster \
    &>/var/log/udocker/kraken/stdout.log &

UBER_CONFIG_DIR=$baseconfig/proxy /usr/bin/kraken-proxy \
    -config=devcluster.yaml \
    -cluster=devcluster \
    -port=5367 \
    -port=5000 \
    &>/var/log/udocker/kraken-proxy/stdout.log &

UBER_CONFIG_DIR=$baseconfig/agent /usr/bin/kraken-agent \
    -peer_ip=localhost \
    -peer_port=8988 \
    -agent_server_port=7602 \
    -config=devcluster.yaml \
    -zone=devcluster \
    -cluster=devcluster \
    &>/var/log/udocker/kraken-agent/stdout.log &

sleep 3

# Poor man's supervisor.
while : ; do
    for c in kraken-testfs kraken-origin kraken-build-index kraken-tracker kraken-proxy kraken-agent; do
        ps aux | grep $c | grep -q -v grep
        status=$?
        if [ $status -ne 0 ]; then
            echo "$c exited unexpectedly. Logs:"
            if [[ "$c" = "kraken-tracker" ]]; then
                c=kraken
            fi
            cat /var/log/udocker/$c/stdout.log
            exit 1
        fi
    done
    sleep 30
done
