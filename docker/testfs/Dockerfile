FROM debian:10

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/cache/kraken/kraken-testfs

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi
USER ${USERNAME}

COPY tools/bin/testfs/testfs /usr/bin/kraken-testfs

WORKDIR /etc/kraken
