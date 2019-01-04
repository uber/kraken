package hashring

import (
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
)

// NoopPassiveRing returns a PassiveRing which never filters unhealthy hosts.
func NoopPassiveRing(hosts hostlist.List) PassiveRing {
	return NewPassive(Config{}, hosts, healthcheck.IdentityFilter{})
}
