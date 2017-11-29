FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD proxy/proxy /usr/bin/kraken-proxy
RUN mkdir -p /var/log/udocker/kraken-proxy

EXPOSE 5054

WORKDIR /root/kraken
