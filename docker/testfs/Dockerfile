FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD tools/bin/testfs/testfs /usr/bin/kraken-testfs
RUN mkdir -p /var/log/udocker/kraken-testfs

WORKDIR /root/kraken
