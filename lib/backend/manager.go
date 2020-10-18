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
	"errors"
	"fmt"
	"regexp"

	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/log"
)

// Manager errors.
var (
	ErrNamespaceNotFound = errors.New("no matches for namespace")
)

type backend struct {
	regexp *regexp.Regexp
	client Client
}

func newBackend(namespace string, c Client) (*backend, error) {
	re, err := regexp.Compile(namespace)
	if err != nil {
		return nil, fmt.Errorf("regexp: %s", err)
	}
	return &backend{
		regexp: re,
		client: c,
	}, nil
}

// Manager manages backend clients for namespace regular expressions.
type Manager struct {
	backends []*backend
}

// NewManager creates a new backend Manager.
func NewManager(configs []Config, auth AuthConfig) (*Manager, error) {
	var backends []*backend
	for _, config := range configs {
		config = config.applyDefaults()
		var c Client

		if len(config.Backend) != 1 {
			return nil, fmt.Errorf("no backend or more than one backend configured")
		}
		var name string
		var backendConfig interface{}
		for name, backendConfig = range config.Backend { // Pull the only key/value out of map
		}
		factory, err := GetFactory(name)
		if err != nil {
			return nil, fmt.Errorf("get backend client factory: %s", err)
		}
		c, err = factory.Create(backendConfig, auth[name])
		if err != nil {
			return nil, fmt.Errorf("create backend client: %s", err)
		}

		if config.Bandwidth.Enable {
			l, err := bandwidth.NewLimiter(config.Bandwidth)
			if err != nil {
				return nil, fmt.Errorf("bandwidth: %s", err)
			}
			c = throttle(c, l)
		}
		b, err := newBackend(config.Namespace, c)
		if err != nil {
			return nil, fmt.Errorf("new backend for namespace %s: %s", config.Namespace, err)
		}
		backends = append(backends, b)
	}
	return &Manager{backends}, nil
}

// AdjustBandwidth adjusts bandwidth limits across all throttled clients to the
// originally configured bandwidth divided by denominator.
func (m *Manager) AdjustBandwidth(denominator int) error {
	for _, b := range m.backends {
		tc, ok := b.client.(*ThrottledClient)
		if !ok {
			continue
		}
		if err := tc.adjustBandwidth(denominator); err != nil {
			return err
		}
		log.With(
			"namespace", b.regexp.String(),
			"ingress", tc.IngressLimit(),
			"egress", tc.EgressLimit(),
			"denominator", denominator).Info("Adjusted backend bandwidth")
	}
	return nil
}

// Register dynamically registers a namespace with a provided client. Register
// should be primarily used for testing purposes -- normally, namespaces should
// be statically configured and provided upon construction of the Manager.
func (m *Manager) Register(namespace string, c Client) error {
	for _, b := range m.backends {
		if b.regexp.String() == namespace {
			return fmt.Errorf("namespace %s already exists", namespace)
		}
	}
	b, err := newBackend(namespace, c)
	if err != nil {
		return fmt.Errorf("new backend: %s", err)
	}
	m.backends = append(m.backends, b)
	return nil
}

// GetClient matches namespace to the configured Client. Returns ErrNamespaceNotFound
// if no clients match namespace.
func (m *Manager) GetClient(namespace string) (Client, error) {
	if namespace == NoopNamespace {
		return NoopClient{}, nil
	}
	for _, b := range m.backends {
		if b.regexp.MatchString(namespace) {
			return b.client, nil
		}
	}
	return nil, ErrNamespaceNotFound
}
