FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/udocker/kraken-testfs
RUN mkdir -p -m 777 /var/cache/udocker/kraken-testfs

ARG USERNAME
ARG USERID
RUN useradd --uid ${USERID} -g ${USERNAME} ${USERNAME}
USER ${USERNAME}

COPY tools/bin/testfs/testfs /usr/bin/kraken-testfs

WORKDIR /home/udocker/kraken
