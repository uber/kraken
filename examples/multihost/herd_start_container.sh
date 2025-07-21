#!/bin/bash

set -ex

# Validate required environment variables
if [ -z "$HERD_HOST_IP" ]; then
    echo "Error: HERD_HOST_IP environment variable is required"
    echo "Usage: HERD_HOST_IP=10.0.1.100 ./herd_start_container.sh"
    exit 1
fi

source examples/multihost/herd_param.sh

echo "Starting Kraken Herd on ${HERD_HOST_IP}..."

# Start kraken herd with multi-host configuration
docker run -d \
    -p ${TESTFS_PORT}:${TESTFS_PORT} \
    -p ${ORIGIN_SERVER_PORT}:${ORIGIN_SERVER_PORT} \
    -p ${ORIGIN_PEER_PORT}:${ORIGIN_PEER_PORT} \
    -p ${TRACKER_PORT}:${TRACKER_PORT} \
    -p ${BUILD_INDEX_PORT}:${BUILD_INDEX_PORT} \
    -p ${PROXY_PORT}:${PROXY_PORT} \
    -p ${PROXY_SERVER_PORT}:${PROXY_SERVER_PORT} \
    -e HERD_HOST_IP=${HERD_HOST_IP} \
    -e HOSTNAME=${HERD_HOST_IP} \
    -v $(pwd)/examples/multihost/config/origin/multihost.yaml:/etc/kraken/config/origin/multihost.yaml \
    -v $(pwd)/examples/multihost/config/tracker/multihost.yaml:/etc/kraken/config/tracker/multihost.yaml \
    -v $(pwd)/examples/multihost/config/build-index/multihost.yaml:/etc/kraken/config/build-index/multihost.yaml \
    -v $(pwd)/examples/multihost/config/proxy/multihost.yaml:/etc/kraken/config/proxy/multihost.yaml \
    -v $(pwd)/examples/multihost/herd_param.sh:/etc/kraken/herd_param.sh \
    -v $(pwd)/examples/multihost/herd_start_processes.sh:/etc/kraken/herd_start_processes.sh \
    --name kraken-herd-multihost \
    kraken-herd:dev ./herd_start_processes.sh

echo "Kraken Herd started successfully!"
echo "Services available at:"
echo "  - Push endpoint: ${HERD_HOST_IP}:15000"
echo "  - Tracker: ${HERD_HOST_IP}:15003"
echo "  - TestFS: ${HERD_HOST_IP}:14000"
