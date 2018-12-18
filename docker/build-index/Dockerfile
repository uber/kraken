FROM debian:9

RUN apt-get update && apt-get install -y curl nginx

RUN mkdir -p -m 777 /var/log/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/cache/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/run/udocker

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh

USER udocker

COPY ./config /home/udocker/kraken-build-index/config
COPY ./nginx/config /home/udocker/kraken-build-index/nginx/config
COPY ./localdb/migrations /home/udocker/kraken-build-index/localdb/migrations
COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./test/tls /home/udocker/kraken/tls

WORKDIR /home/udocker/kraken-build-index
