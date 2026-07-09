#!/bin/bash
# Copy setup.sh to every host in a zone and run it there to install/configure
# nerdctl + stargz-snapshotter.
# Usage: ./rollout_setup.sh [zone]
#   zone  default: dev7

set -eu

ZONE="${1:-dev7}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="$SCRIPT_DIR/setup.sh"

list-zone-hosts() {
  lzc="lzc"
  if echo "$ZONE" | grep -q "dev"; then
    lzc="lzc-crane"
  fi

  "$lzc" host list -z "$ZONE" --format H
}

mapfile -t HOSTS < <(list-zone-hosts)

setup-host() {
    local host="$1"
    echo "=== ${host} ==="
    ssh -o StrictHostKeyChecking=no "prodadmin@${host}" "sudo systemctl stop subd"
    scp -o StrictHostKeyChecking=no "$SETUP_SCRIPT" "prodadmin@${host}:/home/prodadmin/setup.sh"
    ssh -o StrictHostKeyChecking=no "prodadmin@${host}" "chmod +x /home/prodadmin/setup.sh && sudo /home/prodadmin/setup.sh"
    echo "=== ${host} done ==="
}
export -f setup-host
export SETUP_SCRIPT

printf '%s\n' "${HOSTS[@]}" | xargs -P 5 -I{} bash -c 'setup-host "$@"' _ {}
