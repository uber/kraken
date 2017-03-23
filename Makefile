# Flags to pass to go build
BUILD_FLAGS =

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
	client/bin/agent/agent \
	kraken/tracker/tracker

# List all executables
PROGS = \
	kraken/tracker/tracker \
	client/bin/agent/agent \
	test/bin/puller/puller 

kraken/tracker/tracker: kraken/tracker/main.go $(wildcard kraken/tracker/*.go config/tracker/*.go)
client/bin/agent/agent: client/bin/agent/main.go $(wildcard client/*.go)
test/bin/puller/puller: $(wildcard test/bin/puller/*.go)

include go-build/rules.mk

go-build/rules.mk:
		git submodule update --init

.PHONY: rebuild_mocks
rebuild_mocks:
		$(shell mockgen -destination=test/mocks/mock_storage/mock_storage.go code.uber.internal/infra/kraken/kraken/tracker/storage Storage)
		@echo "generated mocks for Storage"

run_tracker: kraken/tracker/tracker run_database
		export UBER_CONFIG_DIR=config/tracker && kraken/tracker/tracker

run_database:
		docker stop mysql-kraken || true
		docker rm mysql-kraken || true
		docker run --name mysql-kraken -p 3306:3306 \
		-e MYSQL_ROOT_PASSWORD=uber -e MYSQL_USER=uber \
		-e MYSQL_PASSWORD=uber -e MYSQL_DATABASE=kraken -v `pwd`/db/data:/var/lib/mysql:rw -d mysql/mysql-server:5.7 && sleep 3
