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
	"io"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"
)

const (
	flushInterval = 100 * time.Millisecond
	flushBytes    = 512
	sampleRate    = 1.0
)

func newStatsdScope(config Config, cluster string) (tally.Scope, io.Closer, error) {
	statter, err := statsd.NewBufferedClient(
		config.Statsd.HostPort, config.Statsd.Prefix, flushInterval, flushBytes)
	if err != nil {
		return nil, nil, err
	}
	r := tallystatsd.NewReporter(statter, tallystatsd.Options{
		SampleRate: sampleRate,
	})
	s, c := tally.NewRootScope(tally.ScopeOptions{
		Reporter: r,
	}, time.Second)
	return s, c, nil
}
