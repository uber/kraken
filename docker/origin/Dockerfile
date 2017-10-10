FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD origin/origin /usr/bin/kraken-origin

EXPOSE 5051 5081

WORKDIR /root/kraken
