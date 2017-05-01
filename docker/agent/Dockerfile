FROM debian:8

EXPOSE 5051 5052 5081 5082

ADD ./ /root/kraken
ADD client/bin/kraken-agent/kraken-agent /usr/bin/kraken-agent

WORKDIR /root/kraken

### Metadata
ENTRYPOINT ["/usr/bin/kraken-agent", "-config=origin/development-local.yaml"]
