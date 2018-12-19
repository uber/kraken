#!/bin/bash

set -ex

for d in /tmp/nginx/ /var/lib/nginx/ /var/log/nginx/ /var/run/nginx/; do
    mkdir -p $d
    chown -R $1:$1 $d
    chmod 0755 $d
done
