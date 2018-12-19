FROM debian:9

RUN apt-get update && apt-get install -y build-essential curl sqlite3 nginx sudo procps

ADD http://download.redis.io/redis-stable.tar.gz /tmp/redis-stable.tar.gz
RUN tar -xvzf /tmp/redis-stable.tar.gz -C /tmp
RUN cd /tmp/redis-stable && make install

RUN mkdir -p -m 777 /var/log/kraken/kraken-agent
RUN mkdir -p -m 777 /var/log/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/log/kraken/kraken-origin
RUN mkdir -p -m 777 /var/log/kraken/kraken-proxy
RUN mkdir -p -m 777 /var/log/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/log/kraken/kraken

RUN mkdir -p -m 777 /var/cache/kraken/kraken-agent
RUN mkdir -p -m 777 /var/cache/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/cache/kraken/kraken-origin
RUN mkdir -p -m 777 /var/cache/kraken/kraken-proxy
RUN mkdir -p -m 777 /var/cache/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/cache/kraken/kraken

RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME
ARG USERID
ARG HOMEDIR
RUN useradd --uid ${USERID} ${USERNAME}

# Allow proxy to run nginx as root.
RUN mkdir -p /etc/sudoers.d/
RUN echo '${USERNAME}  ALL=(root) NOPASSWD: /usr/sbin/nginx' >> /etc/sudoers.d/kraken-proxy

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./agent/agent             /usr/bin/kraken-agent
COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./origin/origin           /usr/bin/kraken-origin
COPY ./proxy/proxy             /usr/bin/kraken-proxy
COPY ./tools/bin/testfs/testfs /usr/bin/kraken-testfs
COPY ./tracker/tracker         /usr/bin/kraken-tracker

WORKDIR ${HOMEDIR}/kraken

EXPOSE 9003
EXPOSE 7602
EXPOSE 5367
EXPOSE 5000
EXPOSE 5263
EXPOSE 8991
EXPOSE 5055
EXPOSE 8351

COPY ./config ${HOMEDIR}/kraken/config
COPY ./nginx/config ${HOMEDIR}/kraken/nginx/config
COPY ./docker/devcluster/start.sh ${HOMEDIR}/kraken/start.sh
COPY ./test/tls ${HOMEDIR}/kraken/tls
CMD ./start.sh
