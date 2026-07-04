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
package httputil

import (
	"crypto/tls"
	"net/http"
)

// ConnPoolConfig tunes a shared *http.Transport. Zero values are replaced with
// safe defaults by NewClientTransport. All fields are YAML-deserializable so
// callers can expose pool sizing through their service config.
type ConnPoolConfig struct {
	MaxIdleConns        int `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost int `yaml:"max_idle_conns_per_host"`
}

func (c *ConnPoolConfig) applyDefaults() {
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 256
	}
	if c.MaxIdleConnsPerHost == 0 {
		c.MaxIdleConnsPerHost = 32
	}
}

// NewClientTransport returns a single *http.Transport suitable for sharing
// across goroutines. If tlsCfg is nil the transport runs in cleartext.
// Callers should construct this once at startup and never mutate it.
func NewClientTransport(tlsCfg *tls.Config, cfg ConnPoolConfig) *http.Transport {
	cfg.applyDefaults()
	return &http.Transport{
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
		TLSClientConfig:     tlsCfg,
	}
}
