FROM debian:9

RUN apt-get update && apt-get install -y curl nginx

RUN mkdir -p -m 777 /var/log/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/cache/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/run/udocker

ARG USERNAME
ARG USERID
RUN useradd --uid ${USERID} -g ${USERNAME} ${USERNAME}

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./config /home/udocker/kraken-build-index/config
COPY ./nginx/config /home/udocker/kraken-build-index/nginx/config
COPY ./localdb/migrations /home/udocker/kraken-build-index/localdb/migrations
COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./test/tls /home/udocker/kraken/tls

WORKDIR /home/udocker/kraken-build-index
