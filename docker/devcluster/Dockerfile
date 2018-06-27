FROM debian:8

RUN apt-get update && apt-get install -y build-essential curl sqlite3

ADD http://download.redis.io/redis-stable.tar.gz /tmp/redis-stable.tar.gz
RUN tar -xvzf /tmp/redis-stable.tar.gz -C /tmp
RUN cd /tmp/redis-stable && make install

RUN mkdir -p -m 777 /var/log/udocker/kraken-agent
RUN mkdir -p -m 777 /var/log/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/log/udocker/kraken-origin
RUN mkdir -p -m 777 /var/log/udocker/kraken-proxy
RUN mkdir -p -m 777 /var/log/udocker/kraken-testfs
RUN mkdir -p -m 777 /var/log/udocker/kraken-tracker

RUN mkdir -p -m 777 /var/cache/udocker/kraken-agent
RUN mkdir -p -m 777 /var/cache/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/cache/udocker/kraken-origin
RUN mkdir -p -m 777 /var/cache/udocker/kraken-proxy
RUN mkdir -p -m 777 /var/cache/udocker/kraken-testfs
RUN mkdir -p -m 777 /var/cache/udocker/kraken-tracker

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

COPY ./ /home/udocker/kraken

COPY agent/agent /usr/bin/kraken-agent
COPY build-index/build-index /usr/bin/kraken-build-index
COPY origin/origin /usr/bin/kraken-origin
COPY proxy/proxy /usr/bin/kraken-proxy
COPY tools/bin/testfs/testfs /usr/bin/kraken-testfs
COPY tracker/tracker /usr/bin/kraken-tracker

WORKDIR /home/udocker/kraken

EXPOSE 9003
EXPOSE 7602

CMD ./docker/devcluster/start.sh
