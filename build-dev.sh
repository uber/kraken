#! /bin/bash

set -vex

docker run --add-host localhost:192.168.65.1 --entrypoint /bin/bash -it --rm -v $UBER_HOME/docker-stuff/etc/ssh/ssh_config:/etc/ssh/ssh_config:ro -v $UBER_HOME/docker-stuff/etc/ssh/ssh_known_hosts:/etc/ssh/ssh_known_hosts:ro -v $UBER_HOME/docker-stuff/etc/uber/environment:/etc/uber/environment:ro -v /Users/evelynl/gocode/src/code.uber.internal/infra/kraken//:/home/udocker/kraken//:ro -v /tmp/ucontainer/:/tmp/ucontainer/ -v $UBER_HOME/docker-stuff/home/udeploy/.ucontainer-secrets-mount/:/home/udeploy/.ucontainer-secrets-mount/ -v /var/run/docker.sock:/var/run/docker.sock -v ssh-agent:/ssh-agent 192.168.65.1:15055/uber-usi/ubuild-container:sjc1-produ-0000000116 ./ubuild/client/development-local.sh devserver build docker kraken /home/udocker/kraken
