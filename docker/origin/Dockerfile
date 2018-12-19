FROM debian:9

RUN apt-get update && apt-get install -y curl sqlite3 nginx

RUN mkdir -p -m 777 /var/log/kraken/kraken-origin
RUN mkdir -p -m 777 /var/cache/kraken/kraken-origin
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME
ARG USERID
ARG HOMEDIR
RUN useradd --uid ${USERID} ${USERNAME}

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./config ${HOMEDIR}/kraken/config
COPY ./nginx/config ${HOMEDIR}/kraken/nginx/config
COPY ./localdb/migrations ${HOMEDIR}/kraken/localdb/migrations
COPY ./test/tls ${HOMEDIR}/kraken/tls
COPY ./origin/origin /usr/bin/kraken-origin

WORKDIR ${HOMEDIR}/kraken
