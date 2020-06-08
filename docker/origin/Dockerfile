FROM debian:10

RUN apt-get update && apt-get install -y curl sqlite3 nginx

RUN mkdir -p -m 777 /var/log/kraken/kraken-origin
RUN mkdir -p -m 777 /var/cache/kraken/kraken-origin
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./origin/origin /usr/bin/kraken-origin
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./localdb/migrations /etc/kraken/localdb/migrations
COPY ./test/tls /etc/kraken/tls

WORKDIR /etc/kraken
