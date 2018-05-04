FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /home/udocker/kraken-build-index
RUN mkdir -p /var/log/udocker/kraken-build-index

WORKDIR /home/udocker/kraken-build-index
