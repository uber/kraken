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
package backend

import (
	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/memsize"
)

// Config defines the union of configuration for all backends. Only one is
// allowed for each config file.
type Config struct {
	Namespace string                 `yaml:"namespace"`
	Backend   map[string]interface{} `yaml:"backend"`

	// If enabled, throttles upload / download bandwidth.
	Bandwidth bandwidth.Config `yaml:"bandwidth"`
}

func (c Config) applyDefaults() Config {
	for k := range c.Backend {
		// TODO: don't hard code backend client name
		if k == "s3" {
			if c.Bandwidth.IngressBitsPerSec == 0 {
				c.Bandwidth.IngressBitsPerSec = 10 * 8 * memsize.Gbit
			}
			if c.Bandwidth.EgressBitsPerSec == 0 {
				c.Bandwidth.EgressBitsPerSec = 8 * memsize.Gbit
			}
		}
	}
	return c
}

// Auth defines auth credentials for corresponding namespaces.
// It has to be different due to langley secrets overlay structure.
type Auth map[string]AuthConfig

// AuthConfig defines the union of authentication credentials for all type of
// remote backends.
type AuthConfig map[string]interface{}
