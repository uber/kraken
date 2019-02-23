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
package healthcheck

import (
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/stringset"
)

// Passive wraps a passive health check and can be used as a hostlist.List. See
// PassiveFilter for passive health check documenation.
type Passive struct {
	hosts  hostlist.List
	filter PassiveFilter
}

// NewPassive returns a new Passive.
func NewPassive(hosts hostlist.List, filter PassiveFilter) *Passive {
	return &Passive{hosts, filter}
}

// Resolve returns the latest healthy hosts. If all hosts are unhealthy, returns
// all hosts.
func (p *Passive) Resolve() stringset.Set {
	all := p.hosts.Resolve()
	healthy := p.filter.Run(all)
	if len(healthy) == 0 {
		return all
	}
	return healthy
}

// Failed marks a request to addr as failed.
func (p *Passive) Failed(addr string) {
	p.filter.Failed(addr)
}
