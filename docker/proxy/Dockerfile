FROM debian:9

RUN apt-get update && apt-get install -y curl nginx

RUN mkdir -p -m 777 /var/log/udocker/kraken-proxy
RUN mkdir -p -m 777 /var/cache/udocker/kraken-proxy
RUN mkdir -p -m 777 /var/run/udocker

ARG USERNAME
ARG USERID
RUN useradd --uid ${USERID} ${USERNAME}

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./config /home/udocker/kraken/config
COPY ./nginx/config /home/udocker/kraken/nginx/config
COPY ./proxy/proxy /usr/bin/kraken-proxy
COPY ./test/tls /home/udocker/kraken/tls

EXPOSE 5054

WORKDIR /home/udocker/kraken
