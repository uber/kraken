package hostlist

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"code.uber.internal/infra/kraken/utils/stringset"
)

// Config defines a list of hosts using either a DNS record or a static list of
// addresses. If present, a DNS record always takes precedence over a static
// list.
type Config struct {
	// DNS record from which to resolve host names.
	DNS string `yaml:"dns"`

	// Statically configured host names.
	Static []string `yaml:"static"`

	// TTL defines how long resolved host lists are cached for.
	TTL time.Duration `yaml:"ttl"`
}

func (c *Config) applyDefaults() {
	if c.TTL == 0 {
		c.TTL = 5 * time.Second
	}
}

// resolve returns either the static list of hosts, or the contents of the dns
// record. If both or neither are supplied, returns error.
func (c *Config) resolve() (stringset.Set, error) {
	if c.DNS == "" && len(c.Static) == 0 {
		return nil, errors.New("no dns record or static list supplied")
	}
	if c.DNS != "" && len(c.Static) > 0 {
		return nil, errors.New("both dns record and static list supplied")
	}
	if len(c.Static) > 0 {
		return stringset.FromSlice(c.Static), nil
	}
	var r net.Resolver
	names, err := r.LookupHost(context.Background(), c.DNS)
	if err != nil {
		return nil, fmt.Errorf("resolve dns: %s", err)
	}
	if len(names) == 0 {
		return nil, errors.New("dns record empty")
	}
	return stringset.FromSlice(names), nil
}
