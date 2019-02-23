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
package originstore

import "time"

// Config defines Store configuration.
type Config struct {
	LocationsTTL         time.Duration `yaml:"locations_ttl"`
	LocationsErrorTTL    time.Duration `yaml:"locations_error_ttl"`
	OriginContextTTL     time.Duration `yaml:"origin_context_ttl"`
	OriginUnavailableTTL time.Duration `yaml:"origin_unavailable_ttl"`
}

func (c *Config) applyDefaults() {
	if c.LocationsTTL == 0 {
		c.LocationsTTL = 10 * time.Second
	}
	if c.LocationsErrorTTL == 0 {
		c.LocationsErrorTTL = time.Second
	}
	if c.OriginContextTTL == 0 {
		c.OriginContextTTL = 10 * time.Second
	}
	if c.OriginUnavailableTTL == 0 {
		c.OriginUnavailableTTL = time.Minute
	}
}
