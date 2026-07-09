#!/bin/bash
# Fix nginx config for large blob uploads on all kraken-origin and kraken-proxy
# instances in a zone: removes size limits, disables request buffering, extends timeouts.
# Usage: ./fix_origin_nginx.sh [zone]
#   zone  default: dev7

set -eu

ZONE="${1:-dev7}"
host_group="kraken-master"

list-kraken-master-hosts() {
  lzc="lzc"
  if echo "$ZONE" | grep -q "dev"; then
    lzc="lzc-crane"
  fi

  "$lzc" host list -z "$ZONE" -g "$host_group" --format H
}

mapfile -t HOSTS < <(list-kraken-master-hosts)

fix_nginx() {
    local container="$1" conf="$2"
    sudo docker exec "$container" sed -i 's/client_max_body_size 10G;/client_max_body_size 0;/' "$conf"
    sudo docker exec "$container" sed -i 's/proxy_read_timeout 3m;/proxy_read_timeout 3m;\n  proxy_send_timeout 3m;\n  client_body_timeout 3m;\n  proxy_request_buffering off;/' "$conf"
    sudo docker exec "$container" grep -E "client_max_body_size|proxy_request_buffering" "$conf"
    sudo docker exec "$container" /usr/sbin/nginx -c "$conf" -s reload
}

for host in "${HOSTS[@]}"; do
    echo "=== ${host} ==="
    ssh -o StrictHostKeyChecking=no prodadmin@${host} bash << EOF
$(declare -f fix_nginx)

ORIGIN=\$(sudo docker ps --format '{{.Names}}' | grep 'kraken-origin_default' | head -1)
PROXY=\$(sudo docker ps --format '{{.Names}}' | grep 'kraken-proxy_default' | head -1)

[ -n "\$ORIGIN" ] && { echo "fixing origin: \$ORIGIN"; fix_nginx "\$ORIGIN" /tmp/nginx/kraken-origin; }
[ -n "\$PROXY"  ] && { echo "fixing proxy:  \$PROXY";  fix_nginx "\$PROXY"  /tmp/nginx/kraken-proxy;  }
echo "done"
EOF
done
