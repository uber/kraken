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
	"context"
	"sync"

	"github.com/uber/kraken/utils/stringset"
)

// Filter filters out unhealthy hosts from a host list.
type Filter interface {
	Run(addrs stringset.Set) stringset.Set
}

type filter struct {
	config  FilterConfig
	checker Checker
	state   *state
}

// NewFilter creates a new Filter. Filter is stateful -- consecutive runs are required
// to detect healthy / unhealthy hosts.
func NewFilter(config FilterConfig, checker Checker) Filter {
	config.applyDefaults()
	return &filter{
		config:  config,
		checker: checker,
		state:   newState(config),
	}
}

// Run applies checker to addrs against the current filter state and returns the
// healthy entries. New entries in addrs not found in the current state are
// assumed as initially healthy. If addrs only contains a single entry, it is
// always considered healthy.
func (f *filter) Run(addrs stringset.Set) stringset.Set {
	if len(addrs) == 1 {
		return addrs.Copy()
	}

	f.state.sync(addrs)

	ctx, cancel := context.WithTimeout(context.Background(), f.config.Timeout)
	defer cancel()

	var wg sync.WaitGroup
	for addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := f.check(ctx, addr); err != nil {
				f.state.failed(addr)
			} else {
				f.state.passed(addr)
			}
		}(addr)
	}
	wg.Wait()

	return f.state.getHealthy()
}

func (f *filter) check(ctx context.Context, addr string) error {
	errc := make(chan error, 1)
	go func() { errc <- f.checker.Check(ctx, addr) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errc:
		return err
	}
}
