FROM debian:8

RUN apt-get update && apt-get install -y curl mysql-client

ADD ./ /root/kraken
ADD tracker/tracker /usr/bin/tracker

EXPOSE 26232

WORKDIR /root/kraken

ENTRYPOINT ["/usr/bin/tracker"]
