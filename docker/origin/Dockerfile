FROM debian:9

RUN apt-get update && apt-get install -y curl sqlite3 nginx

RUN mkdir -p -m 777 /var/log/udocker/kraken-origin
RUN mkdir -p -m 777 /var/cache/udocker/kraken-origin
RUN mkdir -p -m 777 /var/run/udocker

ARG USERNAME
ARG USERID
RUN useradd --uid ${USERID} -g ${USERNAME} ${USERNAME}

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./config /home/udocker/kraken/config
COPY ./nginx/config /home/udocker/kraken/nginx/config
COPY ./localdb/migrations /home/udocker/kraken/localdb/migrations
COPY ./test/tls /home/udocker/kraken/tls
COPY ./origin/origin /usr/bin/kraken-origin

WORKDIR /home/udocker/kraken
