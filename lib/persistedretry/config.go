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
package persistedretry

import "time"

// Config defines Manager configuration.
type Config struct {
	IncomingBuffer int `yaml:"incoming_buffer"`
	RetryBuffer    int `yaml:"retry_buffer"`

	NumIncomingWorkers int `yaml:"num_incoming_workers"`
	NumRetryWorkers    int `yaml:"num_retry_workers"`

	// Max rate of task execution across all workers.
	MaxTaskThroughput time.Duration `yaml:"max_task_throughput"`

	// Interval at which failed tasks should be retried.
	RetryInterval time.Duration `yaml:"retry_interval"`

	// Interval at which retries should be polled from storage.
	PollRetriesInterval time.Duration `yaml:"poll_retries_interval"`

	// Flags that zero-value channel sizes should not have defaults applied.
	Testing bool
}

func (c Config) applyDefaults() Config {
	if c.NumIncomingWorkers == 0 {
		c.NumIncomingWorkers = 4
	}
	if c.NumRetryWorkers == 0 {
		c.NumRetryWorkers = 2
	}
	if c.MaxTaskThroughput == 0 {
		c.MaxTaskThroughput = 10 * time.Millisecond
	}
	if c.PollRetriesInterval == 0 {
		c.PollRetriesInterval = 15 * time.Second
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = 30 * time.Second
	}
	if !c.Testing {
		if c.IncomingBuffer == 0 {
			c.IncomingBuffer = 1000
		}
		if c.RetryBuffer == 0 {
			c.RetryBuffer = 1000
		}
	}
	return c
}
