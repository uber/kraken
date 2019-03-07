FROM kraken-base

USER ${USERNAME}
COPY ./agent/agent /usr/bin/kraken-agent
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./test/tls /etc/kraken/tls

WORKDIR /etc/kraken
