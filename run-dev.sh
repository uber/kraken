#! /bin/bash

set -vex

docker run -d=true --name=kraken -p 5051:5051 -p 5081:5081 -p 8001:8001 uber-usi/kraken:dev
