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
package metrics

import (
	"fmt"
	"io"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
)

func newM3Scope(config Config, cluster string) (tally.Scope, io.Closer, error) {
	if cluster == "" {
		return nil, nil, fmt.Errorf("cluster required for m3")
	}

	if config.M3.Service == "" {
		return nil, nil, fmt.Errorf("service required for m3")
	}

	// HostPort is required for m3 metrics but tally/m3 does not fail when HostPort is empty.
	if config.M3.HostPort == "" {
		return nil, nil, fmt.Errorf("host_port required for m3")
	}

	m3Config := m3.Configuration{
		HostPort: config.M3.HostPort,
		Service:  config.M3.Service,
		Env:      cluster,
	}
	r, err := m3Config.NewReporter()
	if err != nil {
		return nil, nil, err
	}
	s, c := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter: r,
	}, time.Second)
	return s, c, nil
}
