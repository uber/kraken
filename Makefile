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
	build-index/build-index \
	tools/bin/puller/puller \
	tools/bin/benchmarks/benchmarks \
	tools/bin/reload/reload \
	tools/bin/simulation/simulation \
	tools/bin/testfs/testfs \
	tools/bin/trackerload/trackerload

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
build-index/build-index: $(wildcard build-index/*.go)
tools/bin/benchmarks/benchmarks: $(wildcard tools/bin/benchmarks/*.go)
tools/bin/reload/reload: $(wildcard tools/bin/reload/*.go)
tools/bin/simulation/simulation: $(wildcard tools/bin/simulation/*.go)
tools/bin/testfs/testfs: $(wildcard tools/bin/testfs/*.go)
tools/bin/trackerload/trackerload: $(wildcard tools/bin/trackerload/*.go)

.PHONY: bench
bench:
	$(ECHO_V)cd $(FAUXROOT); $(TEST_ENV)	\
		$(GO) test -bench=. -run=$(TEST_DIRS)

test:: redis

jenkins:: redis

mockgen = GOPATH=$(OLDGOPATH) $(GLIDE_EXEC) -g $(GLIDE) -d $(GOPATH)/bin -x github.com/golang/mock/mockgen -- mockgen

# mockgen must be installed on the system to make this work. Install it by running
# `go get github.com/golang/mock/mockgen`.
# go-build/.go/bin/darwin-x86_64/glide-exec is also needed. build it by running
# `cd go-build && make gobuild-bins`
.PHONY: mocks
mocks:
	rm -rf mocks
	mkdir -p $(GOPATH)/bin

	mkdir -p mocks/build-index/remotes
	$(mockgen) \
		-destination=mocks/build-index/remotes/mockremotes.go \
		-package mockremotes \
		code.uber.internal/infra/kraken/build-index/remotes Replicator

	mkdir -p mocks/build-index/tagclient
	$(mockgen) \
		-destination=mocks/build-index/tagclient/mocktagclient.go \
		-package mocktagclient \
		code.uber.internal/infra/kraken/build-index/tagclient Client

	mkdir -p mocks/tracker/announceclient
	$(mockgen) \
		-destination=mocks/tracker/announceclient/mockannounceclient.go \
		-package mockannounceclient \
		code.uber.internal/infra/kraken/tracker/announceclient Client

	mkdir -p mocks/tracker/tagclient
	$(mockgen) \
		-destination=mocks/tracker/tagclient/mocktagclient.go \
		-package mocktagclient \
		code.uber.internal/infra/kraken/tracker/tagclient Client

	mkdir -p mocks/utils/dedup
	$(mockgen) \
		-destination=mocks/utils/dedup/mockdedup.go \
		-package mockdedup \
		code.uber.internal/infra/kraken/utils/dedup Resolver

	mkdir -p mocks/lib/backend
	$(mockgen) \
		-destination=mocks/lib/backend/mockbackend.go \
		-package mockbackend \
		code.uber.internal/infra/kraken/lib/backend Client

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
		code.uber.internal/infra/kraken/lib/store FileStore,FileReadWriter,OriginFileStore

	mkdir -p mocks/lib/torrent/scheduler
	$(mockgen) \
		-destination=mocks/lib/torrent/scheduler/mockscheduler.go \
		-package mockscheduler \
		code.uber.internal/infra/kraken/lib/torrent/scheduler ReloadableScheduler

	mkdir -p mocks/origin/blobclient
	$(mockgen) \
		-destination=mocks/origin/blobclient/mockblobclient.go \
		-package mockblobclient \
		code.uber.internal/infra/kraken/origin/blobclient Client,Provider,ClusterClient,ClientResolver

	mkdir -p mocks/lib/dockerregistry/transfer
	$(mockgen) \
		-destination=mocks/lib/dockerregistry/transfer/mocktransferer.go \
		-package mocktransferer \
		code.uber.internal/infra/kraken/lib/dockerregistry/transfer ImageTransferer

	mkdir -p mocks/tracker/metainfoclient
	$(mockgen) \
		-destination=mocks/tracker/metainfoclient/mockmetainfoclient.go \
		-package mockmetainfoclient \
		code.uber.internal/infra/kraken/tracker/metainfoclient Client

	mkdir -p mocks/os
	$(mockgen) \
		-destination=mocks/os/mockos.go \
		-package mockos \
		os FileInfo

	mkdir -p mocks/net/http
	$(mockgen) \
		-destination=mocks/net/http/mockhttp.go \
		-package mockhttp \
		net/http RoundTripper

# Enumerates all container names, including those created by dockerman.
CONTAINERS := $(foreach \
	c, \
	kraken-redis kraken-tracker kraken-agent kraken-proxy kraken-test-origin-01 \
	kraken-test-origin-02 kraken-test-origin-03 kraken-testfs, \
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

.PHONY: tracker
tracker: redis
	-rm tracker/tracker
	GOOS=linux GOARCH=amd64 make tracker/tracker
	docker build -t kraken-tracker:dev -f docker/tracker/Dockerfile ./

run_tracker: tracker redis
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

.PHONY: agent
agent:
	-rm agent/agent
	GOOS=linux GOARCH=amd64 make agent/agent
	docker build -t kraken-agent:dev -f docker/agent/Dockerfile ./

run_agent: agent
	-docker stop kraken-agent
	-docker rm kraken-agent
	docker run -d \
	    --name=kraken-agent \
		-e UBER_CONFIG_DIR=/root/kraken/config/agent \
		-e UBER_ENVIRONMENT=development \
		-e UBER_DATACENTER=sjc1 \
		-p 5052:5052 \
		-p 5082:5082 \
		kraken-agent:dev \
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

testfs:
	-rm tools/bin/testfs/testfs
	GOOS=linux GOARCH=amd64 make tools/bin/testfs/testfs
	docker build -t kraken-testfs:dev -f docker/testfs/Dockerfile ./

bootstrap_integration:
	if [ ! -d env ]; then \
	   virtualenv --setuptools env ; \
	fi;
	source env/bin/activate
	env/bin/pip install -r requirements-tests.txt

build_integration: tracker origin agent proxy testfs tools/bin/puller/puller docker_stop

run_integration:
	source env/bin/activate
	env/bin/py.test --timeout=120 -v test/python

integration: bootstrap_integration build_integration run_integration

linux-benchmarks:
	-rm tools/bin/benchmarks/benchmarks
	GOOS=linux GOARCH=amd64 make tools/bin/benchmarks/benchmarks

linux-reload:
	-rm tools/bin/reload/reload
	GOOS=linux GOARCH=amd64 make tools/bin/reload/reload

linux-trackerload:
	-rm tools/bin/trackerload/trackerload
	GOOS=linux GOARCH=amd64 make tools/bin/trackerload/trackerload

# jenkins-only debian build job for cli
.PHONY: debian-kraken-cli
debian-kraken-cli: cli/kraken-cli
		make debian-pre

include go-build/rules.mk

go-build/rules.mk:
		git submodule update --init
