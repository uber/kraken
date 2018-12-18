FROM debian:9

RUN apt-get update && apt-get install -y build-essential curl sqlite3 nginx sudo procps

ADD http://download.redis.io/redis-stable.tar.gz /tmp/redis-stable.tar.gz
RUN tar -xvzf /tmp/redis-stable.tar.gz -C /tmp
RUN cd /tmp/redis-stable && make install

RUN mkdir -p -m 777 /var/log/udocker/kraken-agent
RUN mkdir -p -m 777 /var/log/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/log/udocker/kraken-origin
RUN mkdir -p -m 777 /var/log/udocker/kraken-proxy
RUN mkdir -p -m 777 /var/log/udocker/kraken-testfs
RUN mkdir -p -m 777 /var/log/udocker/kraken

RUN mkdir -p -m 777 /var/cache/udocker/kraken-agent
RUN mkdir -p -m 777 /var/cache/udocker/kraken-build-index
RUN mkdir -p -m 777 /var/cache/udocker/kraken-origin
RUN mkdir -p -m 777 /var/cache/udocker/kraken-proxy
RUN mkdir -p -m 777 /var/cache/udocker/kraken-testfs
RUN mkdir -p -m 777 /var/cache/udocker/kraken

RUN mkdir -p -m 777 /var/run/udocker

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
RUN usermod -aG sudo udocker

# Allow proxy to run nginx as root.
RUN mkdir -p /etc/sudoers.d/
RUN echo 'udocker  ALL=(root) NOPASSWD: /usr/sbin/nginx' >> /etc/sudoers.d/kraken-proxy

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh

USER udocker

COPY ./agent/agent             /usr/bin/kraken-agent
COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./origin/origin           /usr/bin/kraken-origin
COPY ./proxy/proxy             /usr/bin/kraken-proxy
COPY ./tools/bin/testfs/testfs /usr/bin/kraken-testfs
COPY ./tracker/tracker         /usr/bin/kraken-tracker

WORKDIR /home/udocker/kraken

EXPOSE 9003
EXPOSE 7602
EXPOSE 5367
EXPOSE 5000
EXPOSE 5263
EXPOSE 8991
EXPOSE 5055
EXPOSE 8351

COPY ./config /home/udocker/kraken/config
COPY ./nginx/config /home/udocker/kraken/nginx/config
COPY ./docker/devcluster/start.sh /home/udocker/kraken/start.sh
COPY ./test/tls /home/udocker/kraken/tls
CMD ./start.sh
