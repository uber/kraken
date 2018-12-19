FROM debian:9

RUN apt-get update && apt-get install -y curl nginx

RUN mkdir -p -m 777 /var/log/kraken/kraken-tracker
RUN mkdir -p -m 777 /var/cache/kraken/kraken-tracker
RUN mkdir -p -m 777 /var/run/kraken

ARG USERNAME
ARG USERID
ARG HOMEDIR
RUN useradd --uid ${USERID} ${USERNAME}

COPY ./docker/setup_nginx.sh /tmp/setup_nginx.sh
RUN /tmp/setup_nginx.sh ${USERNAME}

USER ${USERNAME}

COPY ./config ${HOMEDIR}/kraken/config
COPY ./nginx/config ${HOMEDIR}/kraken/nginx/config
COPY ./test/tls ${HOMEDIR}/kraken/tls
COPY ./tracker/tracker /usr/bin/tracker

WORKDIR ${HOMEDIR}/kraken
