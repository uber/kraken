package hashring

import (
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
)

// Fixture creates a new ring fixture.
func Fixture(addr string) Ring {
	return New(Config{}, healthcheck.NoopList(hostlist.Fixture(addr)))
}
