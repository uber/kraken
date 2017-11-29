FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD origin/origin /usr/bin/kraken-origin
RUN mkdir -p /var/log/udocker/kraken-origin

WORKDIR /root/kraken
