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
	origin/origin \
	tracker/tracker \
	proxy/proxy \
	tools/bin/puller/puller

# List all executables
PROGS = \
	agent/agent \
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

update-golden:
	$(shell UBER_ENVIRONMENT=test UBER_CONFIG_DIR=`pwd`/config/origin go test ./client/cli/ -update 1>/dev/null)
	@echo "generated golden files"

GEN_DIR = .gen/go

PROTO = $(GEN_DIR)/proto/p2p/p2p.pb.go

$(PROTO): $(wildcard proto/*)
	mkdir -p $(GEN_DIR)
	go-build/protoc --plugin=go-build/protoc-gen-go --go_out=$(GEN_DIR) $(subst .pb.go,.proto,$(subst $(GEN_DIR)/,,$@))

tracker/tracker: $(wildcard tracker/*.go)
agent/agent: $(PROTO) $(wildcard agent/*.go)
origin/origin: $(PROTO) $(wildcard origin/*.go)
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

.PHONY: redis
redis:
	-docker stop kraken-redis
	-docker rm kraken-redis
	docker pull redis
	# TODO(codyg): I chose this random port to avoid conflicts in Jenkins. Obviously not ideal.
	docker run -d -p 6380:6379 --name kraken-redis redis:latest

# ==== INIT ====

include go-build/rules.mk

go-build/rules.mk:
	git submodule update --init

# ==== TOOLS ====

# Creates a release summary containing the build revisions of each component
# for the specified version.
releases/%:
	./scripts/release.sh $(subst releases/,,$@)

# Below are simple acceptance tests for quickly checking the validity of newly
# deployed components in production. They detect rudimentary errors, such as
# containers in a crash loop, push failures, invalid tag lists, etc.
#
# WARNING: Manually verify that what you see is what you expect!

.PHONY: acceptance/%

# Runs acceptance tests on an origin host.
acceptance/origin:
	@test $(host)
	@test $(registry) # Registry which test image is marked under on host.
	@test $(repo)
	@test $(tag)
	@test $(namespace)
	ssh $(host) 'bash -s origin' < ./test/acceptance/health.sh
	ssh $(host) 'bash -s proxy' < ./test/acceptance/health.sh
	ssh $(host) "bash -s $(namespace)" < ./test/acceptance/origin.sh
	ssh $(host) "bash -s $(registry) $(repo) $(tag)" < ./test/acceptance/proxy.sh

# Runs acceptance tests on a tracker host.
acceptance/tracker:
	@test $(host)
	@test $(repo)
	@test $(tag)
	@test $(namespace)
	@test $(digest)
	ssh $(host) 'bash -s tracker' < ./test/acceptance/health.sh
	ssh $(host) 'bash -s build-index' < ./test/acceptance/health.sh
	ssh $(host) "bash -s $(repo) $(tag)" < ./test/acceptance/build-index.sh
	ssh $(host) "bash -s $(namespace) $(digest)" < ./test/acceptance/tracker.sh

# Runs acceptance tests on an agent host.
acceptance/agent:
	@test $(host)
	@test $(repo)
	@test $(tag)
	ssh $(host) 'bash -s agent' < ./test/acceptance/health.sh
	ssh $(host) "bash -s $(repo) $(tag)" < ./test/acceptance/agent.sh

# ==== INTEGRATION ====

BUILD_LINUX = GOOS=linux GOARCH=amd64 $(GO) build -i -o $@ $(BUILD_FLAGS) $(BUILD_GC_FLAGS) $(BUILD_VERSION_FLAGS) $(PROJECT_ROOT)/$(dir $@)

# Cross compiling cgo for sqlite3 is not well supported in Mac OSX.
# This workaround builds the binary inside a linux container.
OSX_CROSS_COMPILER = docker run --rm -it -v $(OLDGOPATH):/go -w /go/src/code.uber.internal/infra/kraken golang:latest go build -o ./$@ ./$(dir $@)

LINUX_BINS = \
	agent/agent.linux \
	build-index/build-index.linux \
	origin/origin.linux \
	proxy/proxy.linux \
	tools/bin/testfs/testfs.linux \
	tracker/tracker.linux

$(LINUX_BINS):: $(FAUXFILE) $(FAUX_VENDOR)

agent/agent.linux:: $(PROTO) $(wildcard agent/*.go)
	$(BUILD_LINUX)

build-index/build-index.linux:: $(wildcard build-index/*.go)
	if [[ $$OSTYPE == darwin* ]]; then $(OSX_CROSS_COMPILER); else $(BUILD_LINUX); fi

origin/origin.linux:: $(PROTO) $(wildcard origin/*.go)
	if [[ $$OSTYPE == darwin* ]]; then $(OSX_CROSS_COMPILER); else $(BUILD_LINUX); fi

proxy/proxy.linux:: $(wildcard proxy/*.go)
	$(BUILD_LINUX)

tools/bin/testfs/testfs.linux:: $(wildcard tools/bin/testfs/*.go)
	$(BUILD_LINUX)

tracker/tracker.linux:: $(wildcard tracker/*.go)
	$(BUILD_LINUX)

clean::
	@rm -f $(LINUX_BINS)

.PHONY: docker_stop
docker_stop:
	-docker ps -a --format '{{.Names}}' | grep kraken | while read n; do docker rm -f $$n; done

.PHONY: integration
NAME?=test_
integration: $(LINUX_BINS) tools/bin/puller/puller docker_stop
	docker build -t kraken-agent:dev -f docker/agent/Dockerfile ./
	docker build -t kraken-build-index:dev -f docker/build-index/Dockerfile ./
	docker build -t kraken-origin:dev -f docker/origin/Dockerfile ./
	docker build -t kraken-proxy:dev -f docker/proxy/Dockerfile ./
	docker build -t kraken-testfs:dev -f docker/testfs/Dockerfile ./
	docker build -t kraken-tracker:dev -f docker/tracker/Dockerfile ./
	if [ ! -d env ]; then virtualenv --setuptools env; fi
	source env/bin/activate
	env/bin/pip install -r requirements-tests.txt
	env/bin/py.test --timeout=120 -v test/python -k $(NAME)

.PHONY: runtest
NAME?=test_
runtest: docker_stop
	source env/bin/activate
	env/bin/py.test --timeout=120 -v test/python -k $(NAME)

.PHONY: devcluster
devcluster: $(LINUX_BINS) docker_stop
	docker build -t kraken-devcluster:latest -f docker/devcluster/Dockerfile ./
	docker run -d \
		-p 5263:5263 -p 5367:5367 -p 7602:7602 -p 9003:9003 -p 8991:8991 -p 5055:5055 -p 8351:8351 \
		--hostname localhost --name kraken-devcluster \
		kraken-devcluster:latest
	docker logs -f kraken-devcluster

# ==== MOCKS ====

mockgen = GOPATH=$(OLDGOPATH) $(GLIDE_EXEC) -g $(GLIDE) -d $(GOPATH)/bin -x github.com/golang/mock/mockgen -- mockgen

# mockgen must be installed on the system to make this work. Install it by running
# `go get github.com/golang/mock/mockgen`.
# go-build/.go/bin/darwin-x86_64/glide-exec is also needed. build it by running
# `cd go-build && make gobuild-bins`
.PHONY: mocks
mocks:
	rm -rf mocks
	mkdir -p $(GOPATH)/bin

	mkdir -p mocks/lib/hostlist
	$(mockgen) \
		-destination=mocks/lib/hostlist/mocks.go \
		-package mockhostlist \
		code.uber.internal/infra/kraken/lib/hostlist List

	mkdir -p mocks/lib/healthcheck
	$(mockgen) \
		-destination=mocks/lib/healthcheck/mocks.go \
		-package mockhealthcheck \
		code.uber.internal/infra/kraken/lib/healthcheck Checker,Filter

	mkdir -p mocks/tracker/originstore
	$(mockgen) \
		-destination=mocks/tracker/originstore/mockoriginstore.go \
		-package mockoriginstore \
		code.uber.internal/infra/kraken/tracker/originstore Store

	mkdir -p mocks/build-index/tagstore
	$(mockgen) \
		-destination=mocks/build-index/tagstore/mocktagstore.go \
		-package mocktagstore \
		code.uber.internal/infra/kraken/build-index/tagstore Store,FileStore

	mkdir -p mocks/build-index/tagtype
	$(mockgen) \
		-destination=mocks/build-index/tagtype/mocktagtype.go \
		-package mocktagtype \
		code.uber.internal/infra/kraken/build-index/tagtype Manager,DependencyResolver

	mkdir -p mocks/build-index/tagclient
	$(mockgen) \
		-destination=mocks/build-index/tagclient/mocktagclient.go \
		-package mocktagclient \
		code.uber.internal/infra/kraken/build-index/tagclient Provider,Client

	mkdir -p mocks/tracker/announceclient
	$(mockgen) \
		-destination=mocks/tracker/announceclient/mockannounceclient.go \
		-package mockannounceclient \
		code.uber.internal/infra/kraken/tracker/announceclient Client

	mkdir -p mocks/utils/dedup
	$(mockgen) \
		-destination=mocks/utils/dedup/mockdedup.go \
		-package mockdedup \
		code.uber.internal/infra/kraken/utils/dedup Resolver,TaskRunner,IntervalTask

	mkdir -p mocks/lib/backend
	$(mockgen) \
		-destination=mocks/lib/backend/mockbackend.go \
		-package mockbackend \
		code.uber.internal/infra/kraken/lib/backend Client

	mkdir -p mocks/tracker/peerstore
	$(mockgen) \
		-destination=mocks/tracker/peerstore/mockpeerstore.go \
		-package mockpeerstore \
		code.uber.internal/infra/kraken/tracker/peerstore Store

	mkdir -p mocks/lib/store
	$(mockgen) \
		-destination=mocks/lib/store/mockstore.go \
		-package mockstore \
		code.uber.internal/infra/kraken/lib/store FileReadWriter

	mkdir -p mocks/lib/torrent/scheduler
	$(mockgen) \
		-destination=mocks/lib/torrent/scheduler/mockscheduler.go \
		-package mockscheduler \
		code.uber.internal/infra/kraken/lib/torrent/scheduler ReloadableScheduler,Scheduler

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

	mkdir -p mocks/lib/persistedretry
	$(mockgen) \
		-destination=mocks/lib/persistedretry/mockpersistedretry.go \
		-package mockpersistedretry \
		code.uber.internal/infra/kraken/lib/persistedretry Store,Task,Executor,Manager
	
	mkdir -p mocks/lib/persistedretry/tagreplication
	$(mockgen) \
		-destination=mocks/lib/persistedretry/tagreplication/mocktagreplication.go \
		-package mocktagreplication \
		code.uber.internal/infra/kraken/lib/persistedretry/tagreplication RemoteValidator

# ==== TERRABLOB ====

TERRAMAN_PATH=terraman
export TERRAMAN_CONFIG_FILE?= $(CURDIR)/$(BUILD_DIR)/terraman_host_config.json
TERRAMAN_APP_ID=terraman

$(TERRAMAN_CONFIG_FILE):
	$(TERRAMAN_PATH) start --http -v --app-id=$(TERRAMAN_APP_ID) --port=0 --json-file=$(TERRAMAN_CONFIG_FILE)
	echo "Path to TerraMan Config: $(TERRAMAN_CONFIG_FILE)"

.PHONY: cleanup-terraman
cleanup-terraman:
	rm -f $(TERRAMAN_CONFIG_FILE)
	$(TERRAMAN_PATH) stop-all --app-id=$(TERRAMAN_APP_ID)

# Integration tests, for local testing. TerraMan should be up and running in order this to work
.PHONY: run_terraman
run_terraman: export RUN_INT_TESTS=1
run_terraman: $(FAUX_VENDOR) $(TERRAMAN_CONFIG_FILE)

connect-terrablob-kraken:
		-docker network create integration-net
		-docker network connect integration-net terraman.terrablob
		-docker network connect integration-net kraken-devcluster

.PHONY: terrablob-integration
terrablob-integration: export RUN_TERRABLOB_TESTS=1
terrablob-integration: run_terraman devcluster connect-terrablob-kraken
		$(MAKE) -e -f Makefile test \
		TEST_DIRS="./test/terrablobintegration/" \
		TEST_FLAGS="-timeout=30m" \
		RACE="-race" \
		TEST_VERBOSITY_FLAG="-v"
