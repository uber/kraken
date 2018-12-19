SHELL = /bin/bash -o pipefail
GO = go

# Flags to pass to go build
BUILD_FLAGS = -gcflags '-N -l'

# Where to find your project
PROJECT_ROOT = github.com/uber/kraken

GEN_DIR = .gen/go

PROTO = $(GEN_DIR)/proto/p2p/p2p.pb.go

$(PROTO): $(wildcard proto/*)
	mkdir -p $(GEN_DIR)
	go get -u github.com/golang/protobuf/protoc-gen-go
	protoc --plugin=$(GOPATH)/bin/protoc-gen-go --go_out=$(GEN_DIR) $(subst .pb.go,.proto,$(subst $(GEN_DIR)/,,$@))

# ==== BASIC ====

BUILD_LINUX = GOOS=linux GOARCH=amd64 $(GO) build -i -o $@ $(BUILD_FLAGS) $(BUILD_GC_FLAGS) $(BUILD_VERSION_FLAGS) ./$(dir $@)

# Cross compiling cgo for sqlite3 is not well supported in Mac OSX.
# This workaround builds the binary inside a linux container.
OSX_CROSS_COMPILER = docker run --rm -it -v $(GOPATH):/go -w /go/src/github.com/uber/kraken golang:latest go build -o ./$@ ./$(dir $@)

LINUX_BINS = \
	agent/agent \
	build-index/build-index \
	origin/origin \
	proxy/proxy \
	tools/bin/testfs/testfs \
	tracker/tracker

agent/agent:: $(PROTO) $(wildcard agent/*.go)
	$(BUILD_LINUX)

build-index/build-index:: $(wildcard build-index/*.go)
	if [[ $$OSTYPE == darwin* ]]; then $(OSX_CROSS_COMPILER); else $(BUILD_LINUX); fi

origin/origin:: $(PROTO) $(wildcard origin/*.go)
	if [[ $$OSTYPE == darwin* ]]; then $(OSX_CROSS_COMPILER); else $(BUILD_LINUX); fi

proxy/proxy:: $(wildcard proxy/*.go)
	$(BUILD_LINUX)

tools/bin/testfs/testfs:: $(wildcard tools/bin/testfs/*.go)
	$(BUILD_LINUX)

tracker/tracker:: $(wildcard tracker/*.go)
	$(BUILD_LINUX)

clean::
	@rm -f $(LINUX_BINS)

.PHONY: vendor
vendor:
	glide install

.PHONY: bins
bins: $(LINUX_BINS)

# ==== INTEGRATION ====

test:: redis

jenkins:: redis

.PHONY: redis
redis:
	-docker stop kraken-redis
	-docker rm kraken-redis
	docker pull redis
	# TODO(codyg): I chose this random port to avoid conflicts in Jenkins. Obviously not ideal.
	docker run -d -p 6380:6379 --name kraken-redis redis:latest

.PHONY: docker_stop
docker_stop:
	-docker ps -a --format '{{.Names}}' | grep kraken | while read n; do docker rm -f $$n; done

.PHONY: integration
FILE?=
NAME?=test_
integration: $(LINUX_BINS) tools/bin/puller/puller docker_stop
	docker build -q -t kraken-agent:dev -f docker/agent/Dockerfile ./
	docker build -q -t kraken-build-index:dev -f docker/build-index/Dockerfile ./
	docker build -q -t kraken-origin:dev -f docker/origin/Dockerfile ./
	docker build -q -t kraken-proxy:dev -f docker/proxy/Dockerfile ./
	docker build -q -t kraken-testfs:dev -f docker/testfs/Dockerfile ./
	docker build -q -t kraken-tracker:dev -f docker/tracker/Dockerfile ./
	if [ ! -d env ]; then virtualenv --setuptools env; fi
	source env/bin/activate
	env/bin/pip install -r requirements-tests.txt
	env/bin/py.test --timeout=120 -v -k $(NAME) test/python/$(FILE)

.PHONY: runtest
NAME?=test_
runtest: docker_stop
	source env/bin/activate
	env/bin/py.test --timeout=120 -v -k $(NAME) test/python

.PHONY: devcluster
devcluster: $(LINUX_BINS) docker_stop
	docker build -q -t kraken-devcluster:latest -f docker/devcluster/Dockerfile ./
	docker run -d \
		-p 5000:5000 -p 5263:5263 -p 5367:5367 -p 7602:7602 -p 9003:9003 -p 8991:8991 -p 5055:5055 -p 8351:8351 \
		--hostname 192.168.65.1 --name kraken-devcluster \
		kraken-devcluster:latest
	docker logs -f kraken-devcluster

# ==== TOOLS ====

LINUX_TOOLS = \
	tools/bin/reload/reload \
	tools/bin/simulation/simulation \
	tools/bin/trackerload/trackerload

tools/bin/reload/reload:: $(wildcard tools/bin/reload/reload/*.go)
	$(BUILD_LINUX)

tools/bin/simulation/simulation:: $(wildcard tools/bin/simulation/simulation/*.go)
	$(BUILD_LINUX)

tools/bin/trackerload/trackerload:: $(wildcard tools/bin/trackerload/trackerload/*.go)
	$(BUILD_LINUX)

.PHONY: tools
tools: $(LINUX_TOOLS)

# Creates a release summary containing the build revisions of each component
# for the specified version.
releases/%:
	./scripts/release.sh $(subst releases/,,$@)

.PHONY: bench
bench:
	$(ECHO_V)cd $(FAUXROOT); $(TEST_ENV)	\
		$(GO) test -bench=. -run=$(TEST_DIRS)

# ==== MOCKS ====

mockgen = glide -g $(GLIDE) -d $(GOPATH)/bin -x github.com/golang/mock/mockgen -- mockgen

# mockgen must be installed on the system to make this work. Install it by running
# `go get github.com/golang/mock/mockgen`.
# go-build/.go/bin/darwin-x86_64/glide-exec is also needed. build it by running
# `cd go-build && make gobuild-bins`
.PHONY: mocks
mocks:
	rm -rf mocks
	mkdir -p $(GOPATH)/bin

	mkdir -p mocks/lib/hashring
	$(mockgen) \
		-destination=mocks/lib/hashring/mocks.go \
		-package mockhashring \
		github.com/uber/kraken/lib/hashring Ring,Watcher

	mkdir -p mocks/lib/backend/s3backend
	$(mockgen) \
		-destination=mocks/lib/backend/s3backend/mocks.go \
		-package mocks3backend \
		github.com/uber/kraken/lib/backend/s3backend S3

	# mockgen doesn't play nice when importing vendor code. Must strip the vendor prefix
	# from the imports.
	sed -i '' s,github.com/uber/kraken/vendor/,, mocks/lib/backend/s3backend/mocks.go

	mkdir -p mocks/lib/backend/hdfsbackend/webhdfs
	$(mockgen) \
		-destination=mocks/lib/backend/hdfsbackend/webhdfs/mocks.go \
		-package mockwebhdfs \
		github.com/uber/kraken/lib/backend/hdfsbackend/webhdfs Client

	mkdir -p mocks/lib/hostlist
	$(mockgen) \
		-destination=mocks/lib/hostlist/mocks.go \
		-package mockhostlist \
		github.com/uber/kraken/lib/hostlist List

	mkdir -p mocks/lib/healthcheck
	$(mockgen) \
		-destination=mocks/lib/healthcheck/mocks.go \
		-package mockhealthcheck \
		github.com/uber/kraken/lib/healthcheck Checker,Filter

	mkdir -p mocks/tracker/originstore
	$(mockgen) \
		-destination=mocks/tracker/originstore/mockoriginstore.go \
		-package mockoriginstore \
		github.com/uber/kraken/tracker/originstore Store

	mkdir -p mocks/build-index/tagstore
	$(mockgen) \
		-destination=mocks/build-index/tagstore/mocktagstore.go \
		-package mocktagstore \
		github.com/uber/kraken/build-index/tagstore Store,FileStore

	mkdir -p mocks/build-index/tagtype
	$(mockgen) \
		-destination=mocks/build-index/tagtype/mocktagtype.go \
		-package mocktagtype \
		github.com/uber/kraken/build-index/tagtype DependencyResolver

	mkdir -p mocks/build-index/tagclient
	$(mockgen) \
		-destination=mocks/build-index/tagclient/mocktagclient.go \
		-package mocktagclient \
		github.com/uber/kraken/build-index/tagclient Provider,Client

	mkdir -p mocks/tracker/announceclient
	$(mockgen) \
		-destination=mocks/tracker/announceclient/mockannounceclient.go \
		-package mockannounceclient \
		github.com/uber/kraken/tracker/announceclient Client

	mkdir -p mocks/utils/dedup
	$(mockgen) \
		-destination=mocks/utils/dedup/mockdedup.go \
		-package mockdedup \
		github.com/uber/kraken/utils/dedup TaskRunner,IntervalTask

	mkdir -p mocks/lib/backend
	$(mockgen) \
		-destination=mocks/lib/backend/mockbackend.go \
		-package mockbackend \
		github.com/uber/kraken/lib/backend Client

	mkdir -p mocks/tracker/peerstore
	$(mockgen) \
		-destination=mocks/tracker/peerstore/mockpeerstore.go \
		-package mockpeerstore \
		github.com/uber/kraken/tracker/peerstore Store

	mkdir -p mocks/lib/store
	$(mockgen) \
		-destination=mocks/lib/store/mockstore.go \
		-package mockstore \
		github.com/uber/kraken/lib/store FileReadWriter

	mkdir -p mocks/lib/torrent/scheduler
	$(mockgen) \
		-destination=mocks/lib/torrent/scheduler/mockscheduler.go \
		-package mockscheduler \
		github.com/uber/kraken/lib/torrent/scheduler ReloadableScheduler,Scheduler

	mkdir -p mocks/origin/blobclient
	$(mockgen) \
		-destination=mocks/origin/blobclient/mockblobclient.go \
		-package mockblobclient \
		github.com/uber/kraken/origin/blobclient \
		Client,Provider,ClusterClient,ClusterProvider,ClientResolver

	mkdir -p mocks/lib/dockerregistry/transfer
	$(mockgen) \
		-destination=mocks/lib/dockerregistry/transfer/mocktransferer.go \
		-package mocktransferer \
		github.com/uber/kraken/lib/dockerregistry/transfer ImageTransferer

	mkdir -p mocks/tracker/metainfoclient
	$(mockgen) \
		-destination=mocks/tracker/metainfoclient/mockmetainfoclient.go \
		-package mockmetainfoclient \
		github.com/uber/kraken/tracker/metainfoclient Client

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

	mkdir -p mocks/lib/persistedretry
	$(mockgen) \
		-destination=mocks/lib/persistedretry/mockpersistedretry.go \
		-package mockpersistedretry \
		github.com/uber/kraken/lib/persistedretry Store,Task,Executor,Manager

	mkdir -p mocks/lib/persistedretry/tagreplication
	$(mockgen) \
		-destination=mocks/lib/persistedretry/tagreplication/mocktagreplication.go \
		-package mocktagreplication \
		github.com/uber/kraken/lib/persistedretry/tagreplication RemoteValidator
