# syntax=docker/dockerfile:1.6

# This image combines all central components into one container, for easier
# deployment and management.
ARG TARGETPLATFORM
FROM --platform=$TARGETPLATFORM golang:1.23.11 AS builder

ARG TARGETOS=linux
ARG TARGETARCH

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=1 GO111MODULE=on

WORKDIR /src

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential pkg-config sqlite3 libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN set -euo pipefail; \
    ARCH=${TARGETARCH:-$(dpkg --print-architecture)}; \
    mkdir -p /out && \
    GOOS=${TARGETOS} GOARCH=${ARCH} go build -buildvcs=false -o /out/kraken-build-index ./build-index && \
    GOOS=${TARGETOS} GOARCH=${ARCH} go build -buildvcs=false -o /out/kraken-origin ./origin && \
    GOOS=${TARGETOS} GOARCH=${ARCH} go build -buildvcs=false -o /out/kraken-proxy ./proxy && \
    GOOS=${TARGETOS} GOARCH=${ARCH} go build -buildvcs=false -o /out/kraken-testfs ./tools/bin/testfs && \
    GOOS=${TARGETOS} GOARCH=${ARCH} go build -buildvcs=false -o /out/kraken-tracker ./tracker

FROM debian:12

# Fix repository configuration for Debian 12 (bookworm) with multiple mirrors and better retry logic
RUN echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until && \
    echo 'Acquire::AllowInsecureRepositories "true";' >> /etc/apt/apt.conf.d/99no-check-valid-until && \
    echo 'Acquire::Retries "10";' >> /etc/apt/apt.conf.d/99no-check-valid-until && \
    echo 'Acquire::http::Timeout "60";' >> /etc/apt/apt.conf.d/99no-check-valid-until

RUN apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && \
   ( \
     unset http_proxy https_proxy no_proxy && \
     apt-get update --allow-releaseinfo-change -o Acquire::Retries=10 -o Acquire::http::No-Cache=true -o Acquire::http::timeout=60 -o Acquire::ForceIPv4=true \
   ) \
 && \
   ( \
     unset http_proxy https_proxy no_proxy && \
     apt-get install -y --no-install-recommends --fix-missing --allow-unauthenticated \
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

COPY --from=builder /out/kraken-build-index /usr/bin/kraken-build-index
COPY --from=builder /out/kraken-origin /usr/bin/kraken-origin
COPY --from=builder /out/kraken-proxy /usr/bin/kraken-proxy
COPY --from=builder /out/kraken-testfs /usr/bin/kraken-testfs
COPY --from=builder /out/kraken-tracker /usr/bin/kraken-tracker

RUN chmod +x /usr/bin/kraken-build-index && \
    chmod +x /usr/bin/kraken-origin && \
    chmod +x /usr/bin/kraken-proxy && \
    chmod +x /usr/bin/kraken-testfs && \
    chmod +x /usr/bin/kraken-tracker

USER ${USERNAME}

WORKDIR /etc/kraken

COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./test/tls /etc/kraken/tls
