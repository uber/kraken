FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD proxy/proxy /usr/bin/kraken-proxy

EXPOSE 5054

WORKDIR /root/kraken
