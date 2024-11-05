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
package backend

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/log"

	"github.com/uber-go/tally"
)

// Manager errors.
var (
	ErrNamespaceNotFound = errors.New("no matches for namespace")
)

const isReadyNamespace = "isReadyNamespace"
const isReadyName = "38a03d499119bc417b8a6a016f2cb4540b9f9cc0c13e4da42a73867120d3e908"

type backend struct {
	regexp    *regexp.Regexp
	client    Client
	mustReady bool
}

func newBackend(namespace string, c Client, mustReady bool) (*backend, error) {
	re, err := regexp.Compile(namespace)
	if err != nil {
		return nil, fmt.Errorf("regexp: %s", err)
	}
	return &backend{
		regexp:    re,
		client:    c,
		mustReady: mustReady,
	}, nil
}

// Manager manages backend clients for namespace regular expressions.
type Manager struct {
	backends []*backend
}

// NewManager creates a new backend Manager.
func NewManager(configs []Config, auth AuthConfig, stats tally.Scope) (*Manager, error) {
	var backends []*backend
	for _, config := range configs {
		config = config.applyDefaults()
		var c Client

		if len(config.Backend) != 1 {
			return nil, fmt.Errorf("no backend or more than one backend configured")
		}
		var backendName string
		var backendConfig interface{}
		for backendName, backendConfig = range config.Backend { // Pull the only key/value out of map
		}
		factory, err := getFactory(backendName)
		if err != nil {
			return nil, fmt.Errorf("get backend client factory: %s", err)
		}
		c, err = factory.Create(backendConfig, auth, stats)
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
		b, err := newBackend(config.Namespace, c, config.MustReady)
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
func (m *Manager) Register(namespace string, c Client, mustReady bool) error {
	for _, b := range m.backends {
		if b.regexp.String() == namespace {
			return fmt.Errorf("namespace %s already exists", namespace)
		}
	}
	b, err := newBackend(namespace, c, mustReady)
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

// IsReady returns whether the backends are ready (reachable).
// A backend must be explicitly configured as required for readiness to be checked.
func (m *Manager) CheckReadiness() error {
	for _, b := range m.backends {
		if !b.mustReady {
			continue
		}
		_, err := b.client.Stat(ReadinessCheckNamespace, ReadinessCheckName)
		if err != nil && err != backenderrors.ErrBlobNotFound {
			return fmt.Errorf("backend for namespace '%s' not ready: %s", b.regexp.String(), err)
		}
	}
	return nil
}
