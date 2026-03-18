// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/uber/kraken/utils/log"

	"github.com/uber-go/tally"
)

func init() {
	register("statsd", newStatsdScope)
	register("disabled", newDisabledScope)
	register("m3", newM3Scope)
}

var _scopeFactories = make(map[string]scopeFactory)

type scopeFactory func(config Config, cluster string) (tally.Scope, io.Closer, error)

func register(name string, f scopeFactory) {
	if _, ok := _scopeFactories[name]; ok {
		log.Fatalf("Metrics reporter factory %q is already registered", name)
	}
	_scopeFactories[name] = f
}

// New creates a new metrics Scope from config. If no backend is configured, metrics
// are disabled.
func New(config Config, cluster string) (tally.Scope, io.Closer, error) {
	if config.Backend == "" {
		config.Backend = "disabled"
	}
	f, ok := _scopeFactories[config.Backend]
	if !ok || f == nil {
		return nil, nil, fmt.Errorf("metrics backend %q not registered", config.Backend)
	}
	return f(config, cluster)
}
