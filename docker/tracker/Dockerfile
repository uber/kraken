FROM kraken-base

USER ${USERNAME}

COPY ./tracker/tracker /usr/bin/kraken-tracker
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./test/tls /etc/kraken/tls

WORKDIR /etc/kraken
