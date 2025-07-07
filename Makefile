SHELL = /bin/bash -o pipefail
GO = go

# Flags to pass to go build
BUILD_FLAGS = -gcflags '-N -l'
BUILD_QUIET ?= -q

GOLANG_IMAGE ?= golang:1.14
GOPROXY ?= $(shell go env GOPROXY)

# Where to find your project
PROJECT_ROOT = github.com/uber/kraken
PACKAGE_VERSION ?= $(shell git describe --always --tags)

ALL_SRC = $(shell find . -name "*.go" | grep -v \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*" \
	-e ".*/*.pb.go")

ALL_PKGS = $(shell go list $(sort $(dir $(ALL_SRC))))

# ==== BASIC ====

ifdef RUNNER_WORKSPACE
REPO_ROOT := $(RUNNER_WORKSPACE)/kraken
else
REPO_ROOT := $(CURDIR)
endif

UNAME_S := $(shell uname -s)

# Cross compiling cgo for sqlite3 is not well supported in Mac OSX.
# This workaround builds the binary inside a linux container.
# However, for tools like puller that don't use cgo, we can build natively on macOS.
CROSS_COMPILER = \
  docker run --rm \
    -v $(REPO_ROOT):/app \
    -w /app \
    -e GIT_SSL_NO_VERIFY=true \
    -e GOPROXY=$(GOPROXY) \
    -e GOSUMDB=off \
    -e GOINSECURE="*" \
    -e GO111MODULE=on \
    $(GOLANG_IMAGE) \
    go build -o ./$@ ./$(dir $@);

NATIVE_COMPILER = GOOS=$(shell echo $(UNAME_S) | tr '[:upper:]' '[:lower:]') GOARCH=amd64 go build -o $@ ./$(dir $@)

# Tools that can be built natively on macOS
NATIVE_TOOLS = tools/bin/puller/puller tools/bin/reload/reload tools/bin/visualization/visualization

# Binaries that require Linux build
LINUX_BINS = \
    agent/agent \
    build-index/build-index \
    origin/origin \
    proxy/proxy \
    tools/bin/testfs/testfs \
    tracker/tracker

REGISTRY ?= gcr.io/uber-container-tools

$(LINUX_BINS): $(ALL_SRC)
	$(CROSS_COMPILER)

$(NATIVE_TOOLS): $(ALL_SRC)
ifeq ($(UNAME_S),Darwin)
	$(NATIVE_COMPILER)
else
	$(CROSS_COMPILER)
endif

define tag_image
	docker tag $(1):$(PACKAGE_VERSION) $(1):dev
	docker tag $(1):$(PACKAGE_VERSION) $(REGISTRY)/$(1):$(PACKAGE_VERSION)
endef

.PHONY: images
images: $(LINUX_BINS)
	docker build $(BUILD_QUIET) -t kraken-agent:$(PACKAGE_VERSION) -f docker/agent/Dockerfile ./
	docker build $(BUILD_QUIET) -t kraken-build-index:$(PACKAGE_VERSION) -f docker/build-index/Dockerfile ./
	docker build $(BUILD_QUIET) -t kraken-origin:$(PACKAGE_VERSION) -f docker/origin/Dockerfile ./
	docker build $(BUILD_QUIET) -t kraken-proxy:$(PACKAGE_VERSION) -f docker/proxy/Dockerfile ./
	docker build $(BUILD_QUIET) -t kraken-testfs:$(PACKAGE_VERSION) -f docker/testfs/Dockerfile ./
	docker build $(BUILD_QUIET) -t kraken-tracker:$(PACKAGE_VERSION) -f docker/tracker/Dockerfile ./
	docker build $(BUILD_QUIET) -t kraken-herd:$(PACKAGE_VERSION) -f docker/herd/Dockerfile ./
	$(call tag_image,kraken-agent)
	$(call tag_image,kraken-build-index)
	$(call tag_image,kraken-origin)
	$(call tag_image,kraken-proxy)
	$(call tag_image,kraken-testfs)
	$(call tag_image,kraken-tracker)
	$(call tag_image,kraken-herd)

.PHONY: publish
publish: images
	docker push $(REGISTRY)/kraken-agent:$(PACKAGE_VERSION)
	docker push $(REGISTRY)/kraken-build-index:$(PACKAGE_VERSION)
	docker push $(REGISTRY)/kraken-origin:$(PACKAGE_VERSION)
	docker push $(REGISTRY)/kraken-proxy:$(PACKAGE_VERSION)
	docker push $(REGISTRY)/kraken-testfs:$(PACKAGE_VERSION)
	docker push $(REGISTRY)/kraken-tracker:$(PACKAGE_VERSION)
	docker push $(REGISTRY)/kraken-herd:$(PACKAGE_VERSION)

clean::
	@rm -f $(LINUX_BINS)

.PHONY: bins
bins: $(LINUX_BINS)

# ==== TEST ====
.PHONY: unit-test
unit-test:
	-rm coverage.txt
	$(GO) test -timeout=30s -race -coverprofile=coverage.txt $(ALL_PKGS) --tags "unit"

.PHONY: docker_stop
docker_stop:
	-docker ps -a --format '{{.Names}}' | grep kraken | while read n; do docker rm -f $$n; done

.PHONY: clean_venv
clean_venv:
	rm -rf venv

venv: clean_venv requirements-tests.txt
	python3 -m venv venv
	venv/bin/pip install --upgrade pip setuptools
	venv/bin/pip install -r requirements-tests.txt

.PHONY: integration
FILE?=
NAME?=test_
USERNAME:=$(shell id -u -n)
USERID:=$(shell id -u)
integration: venv $(LINUX_BINS) docker_stop tools/bin/puller/puller
	docker build $(BUILD_QUIET) -t kraken-agent:$(PACKAGE_VERSION) -f docker/agent/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build $(BUILD_QUIET) -t kraken-build-index:$(PACKAGE_VERSION) -f docker/build-index/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build $(BUILD_QUIET) -t kraken-origin:$(PACKAGE_VERSION) -f docker/origin/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build $(BUILD_QUIET) -t kraken-proxy:$(PACKAGE_VERSION) -f docker/proxy/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build $(BUILD_QUIET) -t kraken-testfs:$(PACKAGE_VERSION) -f docker/testfs/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	docker build $(BUILD_QUIET) -t kraken-tracker:$(PACKAGE_VERSION) -f docker/tracker/Dockerfile --build-arg USERID=$(USERID) --build-arg USERNAME=$(USERNAME) ./
	cd test && PYTHONPATH=. PACKAGE_VERSION=$(PACKAGE_VERSION) PYTHONWARNINGS=ignore ../venv/bin/python3 -m pytest --timeout=120 -v -k $(NAME) python/$(FILE)

.PHONY: runtest
NAME?=test_
runtest: venv docker_stop
	cd test && PYTHONPATH=. PYTHONWARNINGS=ignore ../venv/bin/python3 -m pytest --timeout=120 -v -k $(NAME) python

.PHONY: devcluster
devcluster: $(LINUX_BINS) docker_stop images
	./examples/devcluster/herd_start_container.sh
	./examples/devcluster/agent_one_start_container.sh
	./examples/devcluster/agent_two_start_container.sh

# ==== TOOLS ====

TOOLS = \
	tools/bin/puller/puller \
	tools/bin/reload/reload \
	tools/bin/visualization/visualization

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
	$(PROTOC_BIN) --plugin=$(shell go env GOPATH)/bin/protoc-gen-go --go_out=$(GEN_DIR) $(subst .pb.go,.proto,$(subst $(GEN_DIR)/,,$(PROTO)))

# mockgen must be installed on the system to make this work.
# Install it by running:
# `go get github.com/golang/mock/mockgen`.
mockgen = $(shell go env GOPATH)/bin/mockgen

define lowercase
$(shell tr '[:upper:]' '[:lower:]' <<< $(1))
endef

define add_mock
	mkdir -p mocks/$(1)
	$(mockgen) \
		-destination=mocks/$(1)/$(call lowercase,$(2)).go \
		-package mock$(notdir $(1)) \
		$(PROJECT_ROOT)/$(1) $(2)
endef

.PHONY: mocks
mocks:
	rm -rf mocks
	mkdir -p $(shell go env GOPATH)/bin

	$(call add_mock,agent/agentclient,Client)

	$(call add_mock,lib/backend/s3backend,S3)
	# mockgen doesn't play nice when importing vendor code. Must strip the vendor prefix
	# from the imports.
	sed -i '' s,github.com/uber/kraken/vendor/,, mocks/lib/backend/s3backend/s3.go

	$(call add_mock,lib/backend/gcsbackend,GCS)
	sed -i '' s,github.com/uber/kraken/vendor/,, mocks/lib/backend/gcsbackend/gcs.go

	$(call add_mock,lib/hashring,Ring)
	$(call add_mock,lib/hashring,Watcher)

	$(call add_mock,lib/backend/hdfsbackend/webhdfs,Client)

	$(call add_mock,lib/hostlist,List)

	$(call add_mock,lib/healthcheck,Checker)
	$(call add_mock,lib/healthcheck,Filter)
	$(call add_mock,lib/healthcheck,PassiveFilter)

	$(call add_mock,tracker/originstore,Store)

	$(call add_mock,build-index/tagstore,Store)
	$(call add_mock,build-index/tagstore,FileStore)

	$(call add_mock,build-index/tagtype,DependencyResolver)

	$(call add_mock,build-index/tagclient,Provider)
	$(call add_mock,build-index/tagclient,Client)

	$(call add_mock,tracker/announceclient,Client)

	$(call add_mock,utils/dedup,TaskRunner)
	$(call add_mock,utils/dedup,IntervalTask)

	$(call add_mock,lib/backend,Client)

	$(call add_mock,tracker/peerstore,Store)

	$(call add_mock,lib/store,FileReadWriter)

	$(call add_mock,lib/torrent/scheduler,ReloadableScheduler)
	$(call add_mock,lib/torrent/scheduler,Scheduler)

	$(call add_mock,origin/blobclient,Client)
	$(call add_mock,origin/blobclient,Provider)
	$(call add_mock,origin/blobclient,ClusterClient)
	$(call add_mock,origin/blobclient,ClusterProvider)
	$(call add_mock,origin/blobclient,ClientResolver)

	$(call add_mock,lib/containerruntime,Factory)
	$(call add_mock,lib/containerruntime/containerd,Client)
	$(call add_mock,lib/containerruntime/dockerdaemon,DockerClient)
	$(call add_mock,lib/dockerregistry/transfer,ImageTransferer)

	$(call add_mock,tracker/metainfoclient,Client)

	$(call add_mock,lib/persistedretry,Store)
	$(call add_mock,lib/persistedretry,Task)
	$(call add_mock,lib/persistedretry,Executor)
	$(call add_mock,lib/persistedretry,Manager)

	$(call add_mock,lib/persistedretry/tagreplication,RemoteValidator)

	$(call add_mock,utils/httputil,RoundTripper)

# ==== MISC ====

kubecluster:
	cd ./examples/k8s && bash deploy.sh

.PHONY: docs
docs:
	@./scripts/mkdocs.sh -q serve
