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
package tracing

// Config defines tracing configuration.
type Config struct {
	// Enabled enables/disables tracing. Default: false.
	Enabled bool `yaml:"enabled"`

	// ServiceName is the name shown in Jaeger UI. Required if enabled.
	ServiceName string `yaml:"service_name"`

	// AgentHost is the Jaeger/OTLP collector host. Default: localhost.
	AgentHost string `yaml:"agent_host"`

	// AgentPort is the OTLP HTTP port (Jaeger supports OTLP on 4318). Default: 4318.
	AgentPort int `yaml:"agent_port"`

	// SamplingRate is the fraction of traces to sample (0.0 to 1.0). Default: 0.1.
	SamplingRate float64 `yaml:"sampling_rate"`
}

func (c Config) applyDefaults() Config {
	if c.AgentHost == "" {
		c.AgentHost = "localhost"
	}
	if c.AgentPort == 0 {
		c.AgentPort = 4318 // OTLP HTTP default port
	}
	if c.SamplingRate == 0 {
		c.SamplingRate = 0.1
	}
	return c
}
