FROM debian:8

RUN apt-get update && apt-get install -y build-essential curl sqlite3 nginx sudo

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

RUN mkdir -p -m 777 /var/run/udocker

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
RUN usermod -aG sudo udocker

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh

USER udocker

COPY agent/agent.linux             /usr/bin/kraken-agent
COPY build-index/build-index.linux /usr/bin/kraken-build-index
COPY origin/origin.linux           /usr/bin/kraken-origin
COPY proxy/proxy.linux             /usr/bin/kraken-proxy
COPY tools/bin/testfs/testfs.linux /usr/bin/kraken-testfs
COPY tracker/tracker.linux         /usr/bin/kraken-tracker

WORKDIR /home/udocker/kraken

EXPOSE 9003
EXPOSE 7602
EXPOSE 5367
EXPOSE 5263
EXPOSE 8991

COPY ./config /home/udocker/kraken/config
COPY ./nginx/config /home/udocker/kraken/nginx/config
COPY ./docker/devcluster/start.sh /home/udocker/kraken/start.sh
CMD ./start.sh
