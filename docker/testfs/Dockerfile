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
       curl \
   ) \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p -m 777 /var/log/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/cache/kraken/kraken-testfs

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi
USER ${USERNAME}

COPY tools/bin/testfs/testfs /usr/bin/kraken-testfs

WORKDIR /etc/kraken
