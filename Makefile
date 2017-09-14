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
	tools/bin/kraken-cli/kraken


# define the list of proto buffers the service depends on
PROTO_GENDIR ?= .gen
PROTO_SRCS = client/torrent/proto/p2p/p2p.proto
GOBUILD_DIR = go-build

MAKE_PROTO = go-build/protoc --plugin=go-build/protoc-gen-go --proto_path=$(dir $(patsubst %/,%,$(dir $(pb)))) --go_out=$(PROTO_GENDIR)/go $(pb)

update-golden:
	$(shell UBER_ENVIRONMENT=test UBER_CONFIG_DIR=`pwd`/config/origin go test ./client/cli/ -update 1>/dev/null)
	@echo "generated golden files"

proto:
	@mkdir -p $(PROTO_GENDIR)/go
	cd $(dir $(patsubst %/,%,$(GOBUILD_DIR)))
	$(foreach pb, $(PROTO_SRCS), $(MAKE_PROTO);)

tracker/tracker: tracker/main.go $(wildcard tracker/*.go config/tracker/*.go)
client/bin/kraken-agent/kraken-agent: proto client/bin/kraken-agent/main.go $(wildcard client/*.go)
tools/bin/puller/puller: $(wildcard tools/bin/puller/*.go)
tools/bin/kraken-cli/kraken:  client/cli/kraken-cli.go tools/bin/kraken-cli/main.go config/origin/config.go

.PHONY: bench
bench:
	$(ECHO_V)cd $(FAUXROOT); $(TEST_ENV)	\
		$(GO) test -bench=. -run=$(TEST_DIRS)

REDIS_CONTAINER_NAME := "kraken-redis"

.PHONY: redis
redis:
	-docker stop $(REDIS_CONTAINER_NAME)
	-docker rm $(REDIS_CONTAINER_NAME)
	docker pull redis
	# TODO(codyg): I chose this random port to avoid conflicts in Jenkins. Obviously not ideal.
	docker run -d -p 6380:6379 --name $(REDIS_CONTAINER_NAME) redis:latest

test:: redis

jenkins:: redis

mockgen = GOPATH=$(OLDGOPATH) $(GLIDE_EXEC) -g $(GLIDE) -d $(GOPATH)/bin -x github.com/golang/mock/mockgen -- mockgen

.PHONY: mocks
mocks:
	rm -rf mocks

	mkdir -p mocks/tracker/mockstorage
	$(mockgen) \
		-destination=mocks/tracker/mockstorage/mockstorage.go \
		-package mockstorage \
		code.uber.internal/infra/kraken/tracker/storage Storage	

	mkdir -p mocks/client/torrent/mockstorage
	$(mockgen) \
		-destination=mocks/client/torrent/mockstorage/mockstorage.go \
		-package mockstorage \
		code.uber.internal/infra/kraken/client/torrent/storage Torrent

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
