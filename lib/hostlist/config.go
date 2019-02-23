// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package hostlist

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/uber/kraken/utils/stringset"
)

// Config defines a list of hosts using either a DNS record or a static list of
// addresses. If present, a DNS record always takes precedence over a static
// list.
type Config struct {
	// DNS record from which to resolve host names. Must include port suffix,
	// which will be attached to each host within the record.
	DNS string `yaml:"dns"`

	// Statically configured addresses. Must be in 'host:port' format.
	Static []string `yaml:"static"`

	// TTL defines how long resolved host lists are cached for.
	TTL time.Duration `yaml:"ttl"`
}

func (c *Config) applyDefaults() {
	if c.TTL == 0 {
		c.TTL = 5 * time.Second
	}
}

// getResolver parses the configuration for which resolver to use.
func (c *Config) getResolver() (resolver, error) {
	if c.DNS == "" && len(c.Static) == 0 {
		return nil, errors.New("no dns record or static list supplied")
	}
	if c.DNS != "" && len(c.Static) > 0 {
		return nil, errors.New("both dns record and static list supplied")
	}

	if len(c.Static) > 0 {
		for _, addr := range c.Static {
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return nil, fmt.Errorf("invalid static addr: %s", err)
			}
		}
		return &staticResolver{stringset.FromSlice(c.Static)}, nil
	}

	dns, rawport, err := net.SplitHostPort(c.DNS)
	if err != nil {
		return nil, fmt.Errorf("invalid dns: %s", err)
	}
	port, err := strconv.Atoi(rawport)
	if err != nil {
		return nil, fmt.Errorf("invalid dns port: %s", err)
	}
	return &dnsResolver{dns, port}, nil
}

// resolver resolves parsed configuration into a list of addresses.
type resolver interface {
	resolve() (stringset.Set, error)
}

type staticResolver struct {
	set stringset.Set
}

func (r *staticResolver) resolve() (stringset.Set, error) {
	return r.set, nil
}

func (r *staticResolver) String() string {
	return strings.Join(r.set.ToSlice(), ",")
}

type dnsResolver struct {
	dns  string
	port int
}

func (r *dnsResolver) resolve() (stringset.Set, error) {
	var nr net.Resolver
	names, err := nr.LookupHost(context.Background(), r.dns)
	if err != nil {
		return nil, fmt.Errorf("resolve dns: %s", err)
	}
	if len(names) == 0 {
		return nil, errors.New("dns record empty")
	}
	addrs, err := attachPortIfMissing(stringset.FromSlice(names), r.port)
	if err != nil {
		return nil, fmt.Errorf("attach port to dns contents: %s", err)
	}
	return addrs, nil
}

func (r *dnsResolver) String() string {
	return fmt.Sprintf("%s:%d", r.dns, r.port)
}
