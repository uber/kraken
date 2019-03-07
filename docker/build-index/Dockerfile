FROM kraken-base

USER ${USERNAME}

COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken-build-index/nginx/config
COPY ./localdb/migrations /etc/kraken-build-index/localdb/migrations
COPY ./test/tls /etc/kraken/tls

WORKDIR /etc/kraken-build-index
