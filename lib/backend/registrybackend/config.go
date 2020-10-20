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
package registrybackend

import (
	"net"
	"net/http"
	"time"

	"github.com/uber/kraken/lib/backend/registrybackend/security"
)

// Config defines the registry address, timeout and security options.
type Config struct {
	Address string        `yaml:"address"`
	Timeout time.Duration `yaml:"timeout"`
	// ConnectTimeout limits the time spent establishing the TCP connection (if a new one is needed).
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	// ResponseHeaderTimeout limits the time spent reading the headers of the response.
	ResponseHeaderTimeout time.Duration   `yaml:"response_header_timeout"`
	Security              security.Config `yaml:"security"`
}

// Set default configuration
func (c Config) applyDefaults() Config {
	if c.Timeout == 0 {
		c.Timeout = 60 * time.Second
	}
	return c
}

func (c Config) Authenticator() (security.Authenticator, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	if c.ConnectTimeout != 0 {
		dialer := &net.Dialer{
			Timeout:   c.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}
		transport.DialContext = dialer.DialContext
	}

	if c.ResponseHeaderTimeout != 0 {
		transport.ResponseHeaderTimeout = c.ResponseHeaderTimeout
	}

	return security.NewAuthenticator(c.Address, c.Security, transport)
}
