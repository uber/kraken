# syntax=docker/dockerfile:1.6

FROM golang:1.23.11 AS builder

ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=1 GO111MODULE=on

WORKDIR /src

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential pkg-config sqlite3 libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN mkdir -p /out && \
    go build -buildvcs=false -o /out/kraken-origin ./origin

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
       curl \
       sqlite3 \
       nginx \
   ) \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p -m 777 /var/log/kraken/kraken-origin
RUN mkdir -p -m 777 /var/cache/kraken/kraken-origin
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

COPY --from=builder /out/kraken-origin /usr/bin/kraken-origin
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./localdb/migrations /etc/kraken/localdb/migrations
COPY ./test/tls /etc/kraken/tls

RUN chmod +x /usr/bin/kraken-origin

USER ${USERNAME}

WORKDIR /etc/kraken
