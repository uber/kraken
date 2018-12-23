FROM debian:9

RUN apt-get update && apt-get install -y curl nginx

RUN mkdir -p -m 777 /var/log/kraken/kraken-proxy
RUN mkdir -p -m 777 /var/cache/kraken/kraken-proxy
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME
ARG USERID
RUN useradd --uid ${USERID} ${USERNAME}

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./proxy/proxy /usr/bin/kraken-proxy
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./test/tls /etc/kraken/tls

EXPOSE 5054

WORKDIR /etc/kraken
