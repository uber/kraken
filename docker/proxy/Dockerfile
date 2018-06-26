FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/udocker/kraken-proxy
RUN mkdir -p -m 777 /var/cache/udocker/kraken-proxy

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

ADD ./ /home/udocker/kraken
ADD proxy/proxy /usr/bin/kraken-proxy

EXPOSE 5054

WORKDIR /home/udocker/kraken
