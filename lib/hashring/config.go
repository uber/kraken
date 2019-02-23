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

import "time"

// Config defines Ring configuration.
type Config struct {
	// MaxReplica is the max number of hosts each blob will be replicated across.
	// If MaxReplica is >= the number of hosts in the ring, every host will own
	// every blob.
	MaxReplica int `yaml:"max_replica"`

	// RefreshInterval is the interval at which membership / health information
	// is refreshed during monitoring.
	RefreshInterval time.Duration `yaml:"refresh_interval"`
}

func (c *Config) applyDefaults() {
	if c.MaxReplica == 0 {
		c.MaxReplica = 3
	}
	if c.RefreshInterval == 0 {
		c.RefreshInterval = 10 * time.Second
	}
}
