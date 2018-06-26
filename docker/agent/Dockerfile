FROM debian:8

RUN apt-get update && apt-get install -y curl

RUN mkdir -p -m 777 /var/log/udocker/kraken-agent
RUN mkdir -p -m 777 /var/cache/udocker/kraken-agent

RUN groupadd --gid 100001 udocker
RUN useradd --uid 100001 -g udocker -d /home/udocker -m -s /bin/bash udocker
USER udocker

ADD ./ /home/udocker/kraken
ADD agent/agent /usr/bin/kraken-agent

EXPOSE 5052

WORKDIR /home/udocker/kraken
