FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/udocker/kraken
RUN mkdir -p -m 777 /var/cache/udocker/kraken

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

COPY ./config /home/udocker/kraken/config
COPY tracker/tracker.linux /usr/bin/tracker

EXPOSE 26232

WORKDIR /home/udocker/kraken
