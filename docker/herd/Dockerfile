# This image combines all central components into one container, for easier
# deployment and management.
FROM debian:11

# Use a more reliable mirror
RUN echo "deb http://archive.debian.org/debian bullseye main" > /etc/apt/sources.list \
 && echo "deb http://security.debian.org/debian-security bullseye-security main" >> /etc/apt/sources.list \
 && echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until

RUN apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && \
   ( \
     unset http_proxy https_proxy no_proxy && \
     apt-get update -o Acquire::Retries=5 -o Acquire::http::No-Cache=true -o Acquire::http::timeout=30 \
   ) \
 && \
   ( \
     unset http_proxy https_proxy no_proxy && \
     apt-get install -y --no-install-recommends --fix-missing \
       build-essential \
       curl \
       sqlite3 \
       nginx \
       sudo \
       procps \
   ) \
 && rm -rf /var/lib/apt/lists/*

# Install redis.
ADD http://download.redis.io/redis-stable.tar.gz /tmp/redis-stable.tar.gz
RUN tar -xvzf /tmp/redis-stable.tar.gz -C /tmp
RUN cd /tmp/redis-stable && make install

RUN mkdir -p -m 777 /var/log/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/log/kraken/kraken-origin
RUN mkdir -p -m 777 /var/log/kraken/kraken-proxy
RUN mkdir -p -m 777 /var/log/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/log/kraken/kraken-tracker

RUN mkdir -p -m 777 /var/cache/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/cache/kraken/kraken-origin
RUN mkdir -p -m 777 /var/cache/kraken/kraken-proxy
RUN mkdir -p -m 777 /var/cache/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/cache/kraken/kraken-tracker

RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi

# Allow proxy to run nginx as root.
RUN if [ ${USERID} != "0" ]; then mkdir -p /etc/sudoers.d/ && \
    echo '${USERNAME}  ALL=(root) NOPASSWD: /usr/sbin/nginx' >> /etc/sudoers.d/kraken-proxy; fi

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./origin/origin           /usr/bin/kraken-origin
COPY ./proxy/proxy             /usr/bin/kraken-proxy
COPY ./tools/bin/testfs/testfs /usr/bin/kraken-testfs
COPY ./tracker/tracker         /usr/bin/kraken-tracker

WORKDIR /etc/kraken

COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./test/tls /etc/kraken/tls
