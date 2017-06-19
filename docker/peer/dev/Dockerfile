FROM debian:8

RUN apt-get update && apt-get install -y curl

ADD ./ /root/kraken
ADD client/bin/kraken-agent/kraken-agent /usr/bin/kraken-agent

EXPOSE 5052 5082 15082

WORKDIR /root/kraken

### Metadata
ENTRYPOINT ["/usr/bin/kraken-agent", "-config=agent/development-peer-local.yaml"]
