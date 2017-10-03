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
	agent/agent \
	origin/origin \
	tools/bin/puller/puller

# List all executables
PROGS = \
	tracker/tracker \
	agent/agent \
	origin/origin \
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

tracker/tracker: $(wildcard tracker/*.go)
agent/agent: proto $(wildcard agent/*.go)
origin/origin: $(wildcard origin/*.go)
tools/bin/puller/puller: $(wildcard tools/bin/puller/*.go)
tools/bin/kraken-cli/kraken:  client/cli/kraken-cli.go tools/bin/kraken-cli/main.go config/origin/config.go

.PHONY: bench
bench:
	$(ECHO_V)cd $(FAUXROOT); $(TEST_ENV)	\
		$(GO) test -bench=. -run=$(TEST_DIRS)

test:: redis mysql

jenkins:: redis mysql

mockgen = GOPATH=$(OLDGOPATH) $(GLIDE_EXEC) -g $(GLIDE) -d $(GOPATH)/bin -x github.com/golang/mock/mockgen -- mockgen

.PHONY: mocks
mocks:
	rm -rf mocks
	mkdir -p $(GOPATH)/bin
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

	mkdir -p mocks/origin/client
	$(mockgen) \
		-destination=mocks/origin/client/mockclient.go \
		-package mockclient \
		code.uber.internal/infra/kraken/origin/client BlobTransferer

	mkdir -p mocks/lib/store
	$(mockgen) \
		-destination=mocks/lib/store/mockstore.go \
		-package mockstore \
		code.uber.internal/infra/kraken/lib/store FileStore

# Enumerates all container names, including those created by dockerman.
CONTAINERS := $(foreach \
	c, \
	kraken-mysql kraken-redis kraken-tracker kraken-origin kraken-redis, \
	$(c) no-app-id-dockerman.$(c))

# Runs docker stop and docker rm on each container w/ silenced output.
docker_stop:
	@-$(foreach cmd,stop rm,$(foreach c,$(CONTAINERS),docker $(cmd) $(c) &>/dev/null))

.PHONY: redis
redis:
	-docker stop kraken-redis
	-docker rm kraken-redis
	docker pull redis
	# TODO(codyg): I chose this random port to avoid conflicts in Jenkins. Obviously not ideal.
	docker run -d -p 6380:6379 --name kraken-redis redis:latest

.PHONY: mysql
mysql:
	-docker stop kraken-mysql
	-docker rm kraken-mysql
	docker run \
		--name kraken-mysql \
		-p 3307:3306 \
		-e MYSQL_ROOT_PASSWORD=uber \
		-e MYSQL_USER=uber \
		-e MYSQL_PASSWORD=uber \
		-e MYSQL_DATABASE=kraken \
		-v `pwd`/docker/mysql/my.cnf:/etc/my.cnf \
		-d percona/percona-server:5.6.28
	@echo -n "waiting for mysql to start"
	@until docker exec kraken-mysql mysql -u uber --password=uber -e "use kraken" &> /dev/null; \
		do echo -n "."; sleep 1; done
	@echo

.PHONY: tracker
tracker:
	-rm tracker/tracker
	GOOS=linux GOARCH=amd64 make tracker/tracker
	docker build -t kraken-tracker:dev -f docker/tracker/Dockerfile ./

run_tracker: tracker mysql redis
	-docker stop kraken-tracker
	-docker rm kraken-tracker
	docker run -d \
		--name=kraken-tracker \
	    -e UBER_ENVIRONMENT=development \
		-e UBER_CONFIG_DIR=config/tracker \
		-p 26232:26232 \
		kraken-tracker:dev

.PHONY: origin
origin:
	-rm agent/agent
	GOOS=linux GOARCH=amd64 make agent/agent
	docker build -t kraken-origin:dev -f docker/origin/Dockerfile ./

run_origin: origin
	-docker stop kraken-origin
	-docker rm kraken-origin
	docker run -d \
		--name=kraken-origin \
		-e UBER_CONFIG_DIR=/root/kraken/config/origin \
		-e UBER_ENVIRONMENT=development \
		-e UBER_DATACENTER=sjc1 \
		-p 5051:5051 \
		-p 5081:5081 \
		kraken-origin:dev \
		/usr/bin/kraken-agent --announce_ip=192.168.65.1 --announce_port=5081

.PHONY: peer
peer:
	-rm agent/agent
	GOOS=linux GOARCH=amd64 make agent/agent
	docker build -t kraken-peer:dev -f docker/peer/Dockerfile ./

run_peer: peer
	-docker stop kraken-peer
	-docker rm kraken-peer
	docker run -d \
	    --name=kraken-peer \
		-e UBER_CONFIG_DIR=/root/kraken/config/agent \
		-e UBER_ENVIRONMENT=development \
		-e UBER_DATACENTER=sjc1 \
		-p 5052:5052 \
		-p 5082:5082 \
		kraken-peer:dev \
		/usr/bin/kraken-agent --announce_ip=192.168.65.1 --announce_port=5082

bootstrap_integration:
	if [ ! -d env ]; then \
	   virtualenv --setuptools env ; \
	fi;
	source env/bin/activate
	env/bin/pip install -r requirements-tests.txt

build_integration: tracker origin peer tools/bin/puller/puller docker_stop

run_integration:
	source env/bin/activate
	env/bin/py.test --timeout=120 -v test/python

integration: bootstrap_integration build_integration run_integration

# jenkins-only debian build job
.PHONY: debian-kraken-agent
debian-kraken-agent: agent/agent
		make debian-pre

include go-build/rules.mk

go-build/rules.mk:
		git submodule update --init
