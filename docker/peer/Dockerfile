FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD agent/agent /usr/bin/kraken-agent

EXPOSE 5052 5082

WORKDIR /root/kraken
