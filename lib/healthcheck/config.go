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
	"time"
)

// FilterConfig defines configuration for Filter.
type FilterConfig struct {
	// Fails is the number of consecutive failed health checks for a host to be
	// considered unhealthy.
	Fails int `yaml:"fails"`

	// Passes is the number of consecutive passed health checks for a host to be
	// considered healthy.
	Passes int `yaml:"passes"`

	// Timeout of each individual health check.
	Timeout time.Duration `yaml:"timeout"`
}

func (c *FilterConfig) applyDefaults() {
	if c.Fails == 0 {
		c.Fails = 3
	}
	if c.Passes == 0 {
		c.Passes = 2
	}
	if c.Timeout == 0 {
		c.Timeout = 3 * time.Second
	}
}

// MonitorConfig defines configuration for Monitor.
type MonitorConfig struct {
	Interval time.Duration `yaml:"interval"`
}

func (c *MonitorConfig) applyDefaults() {
	if c.Interval == 0 {
		c.Interval = 10 * time.Second
	}
}

// PassiveFilterConfig defines configuration for PassiveFilter.
type PassiveFilterConfig struct {
	// Fails is the number of failed requests that must occur during the FailTimeout
	// period for a host to be marked as unhealthy.
	Fails int `yaml:"fails"`

	// FailTimeout is the window of time during which Fails must occur for a host
	// to be marked as unhealthy.
	//
	// FailTimeout is also the time for which a server is marked unhealthy.
	FailTimeout time.Duration `yaml:"fail_timeout"`
}

func (c *PassiveFilterConfig) applyDefaults() {
	if c.Fails == 0 {
		c.Fails = 3
	}
	if c.FailTimeout == 0 {
		c.FailTimeout = 5 * time.Minute
	}
}
