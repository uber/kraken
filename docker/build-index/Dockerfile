FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/cache/udocker/kraken-build-index

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

ADD ./ /home/udocker/kraken-build-index
ADD build-index/build-index /usr/bin/kraken-build-index

WORKDIR /home/udocker/kraken-build-index
