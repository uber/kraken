FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD agent/agent /usr/bin/kraken-agent
RUN mkdir -p /var/log/udocker/kraken-agent

EXPOSE 5052

WORKDIR /root/kraken
