SHELL = /bin/bash -o pipefail
GO = go

# Flags to pass to go build
BUILD_FLAGS = -gcflags '-N -l'

# Where to find your project
PROJECT_ROOT = github.com/uber/kraken

ALL_SRC = $(shell find . -name "*.go" | grep -v -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*" \
	-e ".*/*.pb.go")

ALL_PKGS = $(shell go list $(sort $(dir $(ALL_SRC))) | grep -v vendor)

# ==== BASIC ====

BUILD_NATIVE = $(GO) build -i -o $@ $(BUILD_FLAGS) $(BUILD_GC_FLAGS) $(BUILD_VERSION_FLAGS) ./$(dir $@)

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

agent/agent:: $(wildcard agent/*.go)
	$(BUILD_LINUX)

build-index/build-index:: $(wildcard build-index/*.go)
	if [[ $$OSTYPE == darwin* ]]; then $(OSX_CROSS_COMPILER); else $(BUILD_LINUX); fi

origin/origin:: $(wildcard origin/*.go)
	if [[ $$OSTYPE == darwin* ]]; then $(OSX_CROSS_COMPILER); else $(BUILD_LINUX); fi

proxy/proxy:: $(wildcard proxy/*.go)
	$(BUILD_LINUX)

tools/bin/testfs/testfs:: $(wildcard tools/bin/testfs/*.go)
	$(BUILD_LINUX)

tracker/tracker:: $(wildcard tracker/*.go)
	$(BUILD_LINUX)

.PHONY: images
images: $(LINUX_BINS)
	docker build -q -t kraken-agent:dev -f docker/agent/Dockerfile ./
	docker build -q -t kraken-build-index:dev -f docker/build-index/Dockerfile ./
	docker build -q -t kraken-origin:dev -f docker/origin/Dockerfile ./
	docker build -q -t kraken-proxy:dev -f docker/proxy/Dockerfile ./
	docker build -q -t kraken-testfs:dev -f docker/testfs/Dockerfile ./
	docker build -q -t kraken-tracker:dev -f docker/tracker/Dockerfile ./
	docker build -q -t kraken-herd:dev -f docker/herd/Dockerfile ./

clean::
	@rm -f $(LINUX_BINS)

vendor:
	go get -v github.com/Masterminds/glide
	$(GOPATH)/bin/glide install

.PHONY: bins
bins: $(LINUX_BINS)

# ==== TEST ====

.PHONY: redis
redis:
	-docker stop kraken-redis
	-docker rm kraken-redis
	docker pull redis
	# TODO(codyg): I chose this random port to avoid conflicts in Jenkins. Obviously not ideal.
	docker run -d -p 6380:6379 --name kraken-redis redis:latest

.PHONY: unit-test
unit-test: vendor redis
	$(GOPATH)/bin/gocov test $(ALL_PKGS) --tags "unit" | $(GOPATH)/bin/gocov report

.PHONY: docker_stop
docker_stop:
	-docker ps -a --format '{{.Names}}' | grep kraken | while read n; do docker rm -f $$n; done

.PHONY: integration
FILE?=
NAME?=test_
USERNAME:=$(shell id -u -n)
USERID:=$(shell id -u)
integration: vendor $(LINUX_BINS) docker_stop tools/bin/puller/puller
	docker build -q -t kraken-agent:dev -f docker/agent/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build -q -t kraken-build-index:dev -f docker/build-index/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build -q -t kraken-origin:dev -f docker/origin/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build -q -t kraken-proxy:dev -f docker/proxy/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build -q -t kraken-testfs:dev -f docker/testfs/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build -q -t kraken-tracker:dev -f docker/tracker/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
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
devcluster: vendor $(LINUX_BINS) docker_stop images
	./examples/devcluster/herd_start_container.sh
	./examples/devcluster/agent_one_start_container.sh
	./examples/devcluster/agent_two_start_container.sh

# ==== TOOLS ====

NATIVE_TOOLS = \
	tools/bin/puller/puller \
	tools/bin/reload/reload \
	tools/bin/visualization/visualization

tools/bin/puller/puller:: $(wildcard tools/bin/puller/puller/*.go)
	$(BUILD_NATIVE)

tools/bin/reload/reload:: $(wildcard tools/bin/reload/reload/*.go)
	$(BUILD_NATIVE)

tools/bin/visualization/visualization:: $(wildcard tools/bin/visualization/visualization/*.go)
	$(BUILD_NATIVE)

.PHONY: tools
tools: $(NATIVE_TOOLS)

# Creates a release summary containing the build revisions of each component
# for the specified version.
releases/%:
	./scripts/release.sh $(subst releases/,,$@)

# ==== CODE GENERATION ====

# In order for kraken to be imported by other projects, we need to check in all
# the generated code, otherwise dependency management tools would report errors
# caused by missing dependencies of kraken itself.

# protoc must be installed on the system to make this work.
# Install it by by following instructions on:
# https://github.com/protocolbuffers/protobuf.
PROTOC_BIN = protoc

PROTO = $(GEN_DIR)/proto/p2p/p2p.pb.go

GEN_DIR = gen/go

.PHONY: protoc
protoc:
	mkdir -p $(GEN_DIR)
	go get -u github.com/golang/protobuf/protoc-gen-go
	$(PROTOC_BIN) --plugin=$(GOPATH)/bin/protoc-gen-go --go_out=$(GEN_DIR) $(subst .pb.go,.proto,$(subst $(GEN_DIR)/,,$(PROTO)))

# mockgen must be installed on the system to make this work.
# Install it by running:
# `go get github.com/golang/mock/mockgen`.
mockgen = GO111MODULES=on $(GOPATH)/bin/mockgen

define kraken_mockgen
	mkdir -p mocks/$(1)
	$(mockgen) \
		-destination=mocks/$(1)/$(shell tr '[:upper:]' '[:lower:]' <<< $(3)).go \
		-package $(2) \
		$(PROJECT_ROOT)/$(1) $(3)

endef

.PHONY: mocks
mocks:
	rm -rf mocks
	mkdir -p $(GOPATH)/bin

	$(call kraken_mockgen,lib/backend/s3backend,mocks3backend,S3)
	# mockgen doesn't play nice when importing vendor code. Must strip the vendor prefix
	# from the imports.
	sed -i '' s,github.com/uber/kraken/vendor/,, mocks/lib/backend/s3backend/s3.go

	$(call kraken_mockgen,lib/hashring,mockhashring,Ring)
	$(call kraken_mockgen,lib/hashring,mockhashring,Watcher)

	$(call kraken_mockgen,lib/backend/hdfsbackend/webhdfs,mockwebhdfs,Client)

	$(call kraken_mockgen,lib/hostlist,mockhostlist,List)

	$(call kraken_mockgen,lib/healthcheck,mockhealthcheck,Checker)
	$(call kraken_mockgen,lib/healthcheck,mockhealthcheck,Filter)
	$(call kraken_mockgen,lib/healthcheck,mockhealthcheck,PassiveFilter)

	$(call kraken_mockgen,tracker/originstore,mockoriginstore,Store)

	$(call kraken_mockgen,build-index/tagstore,mocktagstore,Store)
	$(call kraken_mockgen,build-index/tagstore,mocktagstore,FileStore)

	$(call kraken_mockgen,build-index/tagtype,mocktagtype,DependencyResolver)

	$(call kraken_mockgen,build-index/tagclient,mocktagclient,Provider)
	$(call kraken_mockgen,build-index/tagclient,mocktagclient,Client)

	$(call kraken_mockgen,tracker/announceclient,mockannounceclient,Client)

	$(call kraken_mockgen,utils/dedup,mockdedup,TaskRunner)
	$(call kraken_mockgen,utils/dedup,mockdedup,IntervalTask)

	$(call kraken_mockgen,lib/backend,mockbackend,Client)

	$(call kraken_mockgen,tracker/peerstore,mockpeerstore,Store)

	$(call kraken_mockgen,lib/store,mockstore,FileReadWriter)

	$(call kraken_mockgen,lib/torrent/scheduler,mockscheduler,ReloadableScheduler)
	$(call kraken_mockgen,lib/torrent/scheduler,mockscheduler,Scheduler)

	$(call kraken_mockgen,origin/blobclient,mockblobclient,Client)
	$(call kraken_mockgen,origin/blobclient,mockblobclient,Provider)
	$(call kraken_mockgen,origin/blobclient,mockblobclient,ClusterClient)
	$(call kraken_mockgen,origin/blobclient,mockblobclient,ClusterProvider)
	$(call kraken_mockgen,origin/blobclient,mockblobclient,ClientResolver)

	$(call kraken_mockgen,lib/dockerregistry/transfer,mocktransferer,ImageTransferer)

	$(call kraken_mockgen,tracker/metainfoclient,mockmetainfoclient,Client)

	$(call kraken_mockgen,lib/persistedretry,mockpersistedretry,Store)
	$(call kraken_mockgen,lib/persistedretry,mockpersistedretry,Task)
	$(call kraken_mockgen,lib/persistedretry,mockpersistedretry,Executor)
	$(call kraken_mockgen,lib/persistedretry,mockpersistedretry,Manager)

	$(call kraken_mockgen,lib/persistedretry/tagreplication,mocktagreplication,RemoteValidator)

	mkdir -p mocks/os
	$(mockgen) -destination=mocks/os/mockos.go -package mockos os FileInfo

	mkdir -p mocks/net/http
	$(mockgen) -destination=mocks/net/http/mockhttp.go -package mockhttp net/http RoundTripper
