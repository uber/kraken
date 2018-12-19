FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/kraken/kraken-testfs
RUN mkdir -p -m 777 /var/cache/kraken/kraken-testfs

ARG USERNAME
ARG USERID
ARG HOMEDIR
RUN useradd --uid ${USERID} ${USERNAME}
USER ${USERNAME}

COPY tools/bin/testfs/testfs /usr/bin/kraken-testfs

WORKDIR ${HOMEDIR}/kraken
