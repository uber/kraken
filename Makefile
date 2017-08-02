# Flags to pass to go build
BUILD_FLAGS = -gcflags '-N -l'

# Environment variables to set before go build
BUILD_ENV=

# Flags to pass to go test
TEST_FLAGS =

# Extra dependencies that the tests use
TEST_DEPS =

# Where to find your project
PROJECT_ROOT = code.uber.internal/infra/kraken

# Tells udeploy what your service name is (set to $(notdir of PROJECT_ROOT))
# by default
SERVICES = \
	tracker/tracker \
	client/bin/kraken-agent/kraken-agent \
	tools/bin/puller/puller

# List all executables
PROGS = \
	tracker/tracker \
	client/bin/kraken-agent/kraken-agent \
	tools/bin/puller/puller \

# define the list of proto buffers the service depends on
PROTO_GENDIR ?= .gen
PROTO_SRCS = client/torrent/proto/p2p/p2p.proto
GOBUILD_DIR = go-build

MAKE_PROTO = go-build/protoc --plugin=go-build/protoc-gen-go --proto_path=$(dir $(patsubst %/,%,$(dir $(pb)))) --go_out=$(PROTO_GENDIR)/go $(pb)

proto:
	@mkdir -p $(PROTO_GENDIR)/go
	cd $(dir $(patsubst %/,%,$(GOBUILD_DIR)))
	$(foreach pb, $(PROTO_SRCS), $(MAKE_PROTO);)

tracker/tracker: tracker/main.go $(wildcard tracker/*.go config/tracker/*.go)
client/bin/kraken-agent/kraken-agent: proto
	client/bin/kraken-agent/main.go $(wildcard client/*.go)
tools/bin/puller/puller: $(wildcard tools/bin/puller/*.go)

.PHONY: rebuild_mocks
rebuild_mocks:
		$(shell mockgen -destination=test/mocks/mock_storage/mock_storage.go code.uber.internal/infra/kraken/tracker/storage Storage)
		@echo "generated mocks for Storage"

run_tracker: tracker/tracker run_database
		export UBER_CONFIG_DIR=config/tracker && tracker/tracker

run_database:
		docker stop mysql-kraken || true
		docker rm mysql-kraken || true
		docker run --name mysql-kraken -p 3306:3306 \
		-e MYSQL_ROOT_PASSWORD=uber -e MYSQL_USER=uber \
		-e MYSQL_PASSWORD=uber -e MYSQL_DATABASE=kraken -d percona/percona-server:5.6.28 && sleep 30

run_agent_origin:
		make clean; GOOS=linux GOARCH=amd64 make client/bin/kraken-agent/kraken-agent
		docker build -t kraken-origin:dev -f docker/origin/Dockerfile ./
		docker stop kraken-origin || true
		docker rm kraken-origin || true
		docker run -d --name=kraken-origin -p 5051:5051 -p 5081:5081 --entrypoint="/root/kraken/scripts/start_origin.sh" kraken-origin:dev

run_agent_peer:
		make clean; GOOS=linux GOARCH=amd64 make client/bin/kraken-agent/kraken-agent
		docker build -t kraken-peer:dev -f docker/peer/dev/Dockerfile ./
		docker stop kraken-peer || true
		docker rm kraken-peer || true
		docker run -d --name=kraken-peer -p 5052:5052 -p 5082:5082 --entrypoint="/root/kraken/scripts/start_peer.sh" kraken-peer:dev

integration:
		make clean
		GOOS=linux GOARCH=amd64 make tracker/tracker
		GOOS=linux GOARCH=amd64 make client/bin/kraken-agent/kraken-agent
		docker build -t kraken-tracker:test -f docker/tracker/Dockerfile ./
		docker build -t kraken-origin:dev -f docker/origin/Dockerfile ./
		docker build -t kraken-peer:test -f docker/peer/test/Dockerfile ./
		make tools/bin/puller/puller
		if [ ! -d env ]; then \
		   virtualenv --setuptools env ; \
		fi;
		env/bin/pip install -r requirements-tests.txt
		make run_integration

run_integration:
	CONFIG_DIR=config/tracker/config env/bin/py.test --timeout=60 -v test/python

# jenkins-only debian build job
.PHONY: debian-kraken-agent
debian-kraken-agent: client/bin/kraken-agent/kraken-agent
		make debian-pre

include go-build/rules.mk

go-build/rules.mk:
		git submodule update --init
