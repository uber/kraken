FROM debian:10

RUN apt-get update && apt-get install -y curl nginx

RUN mkdir -p -m 777 /var/log/kraken/kraken-tracker
RUN mkdir -p -m 777 /var/cache/kraken/kraken-tracker
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME="root"
ARG USERID="0"
RUN if [ ${USERID} != "0" ]; then useradd --uid ${USERID} ${USERNAME}; fi

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./tracker/tracker /usr/bin/kraken-tracker
COPY ./config /etc/kraken/config
COPY ./nginx/config /etc/kraken/nginx/config
COPY ./test/tls /etc/kraken/tls

WORKDIR /etc/kraken
