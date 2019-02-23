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

// Config defines metrics configuration.
type Config struct {
	Backend string       `yaml:"backend"`
	Statsd  StatsdConfig `yaml:"statsd"`
	M3      M3Config     `yaml:"m3"`
}

// StatsdConfig defines statsd configuration.
type StatsdConfig struct {
	HostPort string `yaml:"host_port"`
	Prefix   string `yaml:"prefix"`
}

// M3Config defines m3 configuration.
type M3Config struct {
	HostPort string `yaml:"host_port"`
	Service  string `yaml:"service"`
	Env      string `yaml:"env"`
}
