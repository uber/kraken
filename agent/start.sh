#!/bin/bash

set -vex

if [[ -z "${UBER_DATACENTER}" ]]; then
    NGINX_CONFIG_NAME="kraken-agent-default"
else
    NGINX_CONFIG_NAME="kraken-agent-${UBER_DATACENTER}"
fi
mkdir -p /etc/nginx/sites-available && ln -fs /home/udocker/kraken-agent/agent/nginx/${NGINX_CONFIG_NAME} /etc/nginx/sites-available/${NGINX_CONFIG_NAME}
mkdir -p /etc/nginx/sites-enabled && ln -fs /home/udocker/kraken-agent/agent/nginx/${NGINX_CONFIG_NAME}  /etc/nginx/sites-enabled/${NGINX_CONFIG_NAME}

# If agent crashes, systemd won't restart the container because agent is a background
# process (nginx is the primary process). For this reason, we must monitor the agent
# health in the background and manually intervene if necessary.
/home/udocker/kraken-agent/agent/health_check.sh >> /var/log/udocker/kraken-agent/health_check.log &

/home/udocker/kraken-agent/agent/agent -peer_ip=$KRAKEN_PEER_IP -peer_port=8988 -agent_server_port=7602 -config=$KRAKEN_CONFIG -zone=$UBER_DATACENTER -cluster=$KRAKEN_CLUSTER | tee -a /var/log/udocker/kraken-agent/stdout.log &

sudo /usr/sbin/nginx -g "daemon off;"
