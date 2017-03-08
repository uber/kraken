# Flags to pass to go build
BUILD_FLAGS =

# Environment variables to set before go build
BUILD_ENV= GOOS=linux GOARCH=amd64 GOARM=7

# Flags to pass to go test
TEST_FLAGS =

# Extra dependencies that the tests use
TEST_DEPS =

# Where to find your project
PROJECT_ROOT = code.uber.internal/infra/kraken

# Tells udeploy what your service name is (set to $(notdir of PROJECT_ROOT))
# by default
SERVICES = kraken

# List all executables
PROGS = \
	client/agent/kraken

client/agent/kraken: client/agent/kraken.go $(wildcard client/agent/*.go)

include go-build/rules.mk

go-build/rules.mk:
		git submodule update --init
