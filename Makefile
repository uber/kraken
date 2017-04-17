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
	kraken/tracker/tracker \
	client/bin/kraken-agent/kraken-agent \
	tools/bin/puller/puller

# List all executables
PROGS = \
	kraken/tracker/tracker \
	client/bin/kraken-agent/kraken-agent \
	tools/bin/puller/puller

kraken/tracker/tracker: kraken/tracker/main.go $(wildcard kraken/tracker/*.go config/tracker/*.go)
client/bin/kraken-agent/kraken-agent: client/bin/kraken-agent/main.go $(wildcard client/*.go)
tools/bin/puller/puller: $(wildcard test/bin/puller/*.go)

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
		-e MYSQL_PASSWORD=uber -e MYSQL_DATABASE=kraken -v ${HOME}/kraken/mysql:/var/lib/mysql:rw -d percona/percona-server:5.6.28 && sleep 3

integration:
		make clean; GOOS=linux GOARCH=amd64 make kraken/tracker/tracker
		if [ ! -d env ]; then \
		   virtualenv --setuptools env ; \
		fi;
		env/bin/pip install -r requirements-tests.txt
		CONFIG_DIR=config/tracker/config env/bin/py.test test/python

# jenkins-only debian build job
.PHONY: debian-kraken-agent
debian-kraken-agent: client/bin/kraken-agent/kraken-agent
		make debian-pre
