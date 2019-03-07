FROM kraken-base

USER ${USERNAME}

COPY ./proxy/proxy /usr/bin/kraken-proxy
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./test/tls /etc/kraken/tls

WORKDIR /etc/kraken
