FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/udocker/kraken-testfs
RUN mkdir -p -m 777 /var/cache/udocker/kraken-testfs

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

ADD tools/bin/testfs/testfs /usr/bin/kraken-testfs

WORKDIR /home/udocker/kraken
