FROM debian:12

# Fix repository configuration for Debian 12 (bookworm) with CDN mirror for better stability
RUN echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until && \
    echo 'Acquire::AllowInsecureRepositories "true";' >> /etc/apt/apt.conf.d/99no-check-valid-until && \
    echo 'Acquire::Retries "15";' >> /etc/apt/apt.conf.d/99no-check-valid-until && \
    echo 'Acquire::http::Timeout "120";' >> /etc/apt/apt.conf.d/99no-check-valid-until

RUN apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && \
   ( \
     unset http_proxy https_proxy no_proxy && \
     apt-get update --allow-releaseinfo-change -o Acquire::Retries=15 -o Acquire::http::No-Cache=true -o Acquire::http::timeout=120 -o Acquire::ForceIPv4=true \
   ) \
 && \
   ( \
     unset http_proxy https_proxy no_proxy && \
     apt-get install -y --no-install-recommends --fix-missing --allow-unauthenticated \
       curl \
   ) \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p -m 777 /var/log/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/cache/kraken/kraken-testfs

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi

COPY tools/bin/testfs/testfs /usr/bin/kraken-testfs
RUN chmod +x /usr/bin/kraken-testfs

USER ${USERNAME}

WORKDIR /etc/kraken
