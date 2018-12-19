FROM debian:9

RUN apt-get update && apt-get install -y curl nginx

RUN mkdir -p -m 777 /var/log/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/cache/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME
ARG USERID
ARG HOMEDIR
RUN useradd --uid ${USERID} ${USERNAME}

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./config ${HOMEDIR}/kraken-build-index/config
COPY ./nginx/config ${HOMEDIR}/kraken-build-index/nginx/config
COPY ./localdb/migrations ${HOMEDIR}/kraken-build-index/localdb/migrations
COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./test/tls ${HOMEDIR}/kraken/tls

WORKDIR ${HOMEDIR}/kraken-build-index
