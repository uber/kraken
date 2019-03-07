FROM kraken-base

RUN apt-get update && apt-get install -y sqlite3

USER ${USERNAME}

COPY ./origin/origin /usr/bin/kraken-origin
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./localdb/migrations /etc/kraken/localdb/migrations
COPY ./test/tls /etc/kraken/tls

WORKDIR /etc/kraken
