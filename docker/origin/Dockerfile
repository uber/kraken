FROM debian:8

RUN apt-get update && apt-get install -y curl sqlite3

RUN mkdir -p -m 777 /var/log/udocker/kraken-origin
RUN mkdir -p -m 777 /var/cache/udocker/kraken-origin

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

ADD ./ /home/udocker/kraken
ADD origin/origin /usr/bin/kraken-origin

WORKDIR /home/udocker/kraken
