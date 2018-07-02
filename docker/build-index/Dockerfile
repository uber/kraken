FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/cache/udocker/kraken-build-index

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

COPY ./config /home/udocker/kraken-build-index/config
COPY ./localdb/migrations /home/udocker/kraken-build-index/localdb/migrations
COPY build-index/build-index.linux /usr/bin/kraken-build-index

WORKDIR /home/udocker/kraken-build-index
