#!/bin/bash

set -ex

for d in /etc/nginx/ /var/lib/nginx/ /var/log/nginx/ /var/run/nginx/; do
    mkdir -p $d
    chown -R udocker:udocker $d
    chmod 0755 $d
done

rm -f /etc/nginx/sites-enabled/default
rm -f /etc/nginx/sites-available/default
