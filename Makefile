# Flags to pass to go build
BUILD_FLAGS = -gcflags '-N -l'

# Environment variables to set before go build
BUILD_ENV=

# Flags to pass to go test
TEST_FLAGS = -timeout 2m

# Extra dependencies that the tests use
TEST_DEPS =

# Where to find your project
PROJECT_ROOT = code.uber.internal/infra/kraken

# Tells udeploy what your service name is (set to $(notdir of PROJECT_ROOT))
# by default
SERVICES = \
	agent/agent \
	cli/kraken-cli \
	origin/origin \
	tracker/tracker \
	proxy/proxy \
	tools/bin/puller/puller

# List all executables
PROGS = \
	agent/agent \
	cli/kraken-cli \
	tracker/tracker \
	origin/origin \
	proxy/proxy \
	tools/bin/puller/puller 

# define the list of proto buffers the service depends on
PROTO_GENDIR ?= .gen
PROTO_SRCS = lib/torrent/proto/p2p/p2p.proto
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
origin/origin: proto $(wildcard origin/*.go)
cli/kraken-cli: $(wildcard cli/*.go)
tools/bin/puller/puller: $(wildcard tools/bin/puller/*.go)
proxy/proxy: $(wildcard proxy/*.go)

.PHONY: bench
bench:
	$(ECHO_V)cd $(FAUXROOT); $(TEST_ENV)	\
		$(GO) test -bench=. -run=$(TEST_DIRS)

test:: redis mysql

jenkins:: redis mysql

mockgen = GOPATH=$(OLDGOPATH) $(GLIDE_EXEC) -g $(GLIDE) -d $(GOPATH)/bin -x github.com/golang/mock/mockgen -- mockgen

# mockgen must be installed on the system to make this work. Install it by running
# `go get github.com/golang/mock/mockgen`.
.PHONY: mocks
mocks:
	rm -rf mocks
	mkdir -p $(GOPATH)/bin
	mkdir -p mocks/tracker/mockstorage
	$(mockgen) \
		-destination=mocks/tracker/mockstorage/mockstorage.go \
		-package mockstorage \
		code.uber.internal/infra/kraken/tracker/storage Storage	

	mkdir -p mocks/lib/torrent/mockstorage
	$(mockgen) \
		-destination=mocks/lib/torrent/mockstorage/mockstorage.go \
		-package mockstorage \
		code.uber.internal/infra/kraken/lib/torrent/storage Torrent

	mkdir -p mocks/lib/store
	$(mockgen) \
		-destination=mocks/lib/store/mockstore.go \
		-package mockstore \
		code.uber.internal/infra/kraken/lib/store FileStore,FileReadWriter

	mkdir -p mocks/origin/blobclient
	$(mockgen) \
		-destination=mocks/origin/blobclient/mockblobclient.go \
		-package mockblobclient \
		code.uber.internal/infra/kraken/origin/blobclient Client,Provider,ClusterResolver

	mkdir -p mocks/lib/dockerregistry/transfer
	$(mockgen) \
		-destination=mocks/lib/dockerregistry/transfer/mocktransferer.go \
		-package mocktransferer \
		code.uber.internal/infra/kraken/lib/dockerregistry/transfer ImageTransferer

	mkdir -p mocks/lib/dockerregistry/transfer/manifestclient
	$(mockgen) \
		-destination=mocks/lib/dockerregistry/transfer/manifestclient/mockmanifestclient.go \
		-package mockmanifestclient \
		code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient Client

	mkdir -p mocks/os
	$(mockgen) \
		-destination=mocks/os/mockos.go \
		-package mockos \
		os FileInfo

# Enumerates all container names, including those created by dockerman.
CONTAINERS := $(foreach \
	c, \
	kraken-mysql kraken-redis kraken-tracker kraken-peer kraken-proxy kraken-origin, \
	$(c))

# Runs docker stop and docker rm on each container w/ silenced output.
docker_stop:
	@-$(foreach c,$(CONTAINERS),docker rm -f $$(docker ps -aq --filter name=$(c)) &>/dev/null)

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
	-rm origin/origin
	GOOS=linux GOARCH=amd64 make origin/origin
	docker build -t kraken-origin:dev -f docker/origin/Dockerfile ./

run_origin: origin
	-docker stop kraken-origin
	-docker rm kraken-origin
	docker run -d \
		--name=kraken-origin \
		--hostname=192.168.65.1 \
		-e UBER_CONFIG_DIR=/root/kraken/config/origin \
		-e UBER_ENVIRONMENT=development \
		-e UBER_DATACENTER=sjc1 \
		-p 19003:19003 \
		-p 5081:5081 \
		# Mount cache dir so restart will be able to load from disk
		-v /tmp/kraken:/var/kraken/ \
		kraken-origin:dev \
		/usr/bin/kraken-origin --peer_ip=192.168.65.1 --peer_port=5081 --blobserver_port=19003

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
		/usr/bin/kraken-agent --peer_ip=192.168.65.1 --peer_port=5082

.PHONY: proxy
proxy:
	-rm proxy/proxy
	GOOS=linux GOARCH=amd64 make proxy/proxy
	docker build -t kraken-proxy:dev -f docker/proxy/Dockerfile ./

run_proxy: proxy
	-docker stop kraken-proxy
	-docker rm kraken-proxy
	docker run -d \
		--name=kraken-proxy \
		-e UBER_CONFIG_DIR=/root/kraken/config/proxy \
		-e UBER_ENVIRONMENT=development \
		-e UBER_DATACENTER=sjc1 \
		-p 5054:5054 \
		kraken-proxy:dev \
		/usr/bin/kraken-proxy 

bootstrap_integration:
	if [ ! -d env ]; then \
	   virtualenv --setuptools env ; \
	fi;
	source env/bin/activate
	env/bin/pip install -r requirements-tests.txt

build_integration: tracker origin peer proxy tools/bin/puller/puller docker_stop

run_integration:
	source env/bin/activate
	env/bin/py.test --timeout=120 -v test/python

integration: bootstrap_integration build_integration run_integration

# jenkins-only debian build job for cli
.PHONY: debian-kraken-cli
debian-kraken-cli: cli/kraken-cli
		make debian-pre

include go-build/rules.mk

go-build/rules.mk:
		git submodule update --init
