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
package hashring

 import (
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
)

 // PassiveRing is a wrapper around Ring which supports passive health checks.
// See healthcheck.PassiveFilter for passive health check documentation.
type PassiveRing interface {
	Ring
	Failed(addr string)
}

 type passiveRing struct {
	Ring
	passiveFilter healthcheck.PassiveFilter
}

 // NewPassive creats a new PassiveRing.
func NewPassive(
	config Config,
	cluster hostlist.List,
	passiveFilter healthcheck.PassiveFilter,
	opts ...Option) PassiveRing {

 	return &passiveRing{
		New(config, cluster, passiveFilter, opts...),
		passiveFilter,
	}
}

 // Failed marks a request to addr as failed.
func (p *passiveRing) Failed(addr string) {
	p.passiveFilter.Failed(addr)
}
