FROM debian:12
LABEL org.opencontainers.image.source=https://github.com/uber/kraken
LABEL org.opencontainers.image.licenses=Apache-2.0
LABEL org.opencontainers.image.description="The Kraken build-index component maps a tag to a blob digest. The component stores this mapping on a pluggable backend, for example S3 or GCS. The component supports tag replication between clusters. Production deployments run this component."

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
       nginx \
   ) \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p -m 777 /var/log/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/cache/kraken/kraken-build-index
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

COPY ./build-index/build-index /usr/bin/kraken-build-index
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken-build-index/nginx/config
COPY ./localdb/migrations /etc/kraken-build-index/localdb/migrations
COPY ./test/tls /etc/kraken/tls

RUN chmod +x /usr/bin/kraken-build-index

USER ${USERNAME}

WORKDIR /etc/kraken-build-index
