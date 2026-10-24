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
package tagtype

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/httputil"
)

var errNamespaceNotFound = errors.New("no matches for namespace")

const (
	defaultMaxRetries          = 3
	defaultInitialInterval     = 500 * time.Millisecond
	defaultRandomizationFactor = 0.05
	defaultMultiplier          = 2
	defaultMaxInterval         = 30 * time.Second
)

// Config defines the namespace and the type of resolver associated with it.
type Config struct {
	Namespace string `yaml:"namespace"`
	Type      string `yaml:"type"`

	// Backoff controls how the docker type retries manifest downloads.
	// Retries are always enabled for the docker type: Backoff.Enabled is
	// ignored and set to true. Fields left at zero take the defaults above.
	Backoff httputil.ExponentialBackOffConfig `yaml:"backoff"`
}

func (c Config) applyDefaults() Config {
	// A missing enabled key reads as false, so always enable retries.
	c.Backoff.Enabled = true
	if c.Backoff.InitialInterval == 0 {
		c.Backoff.InitialInterval = defaultInitialInterval
	}
	if c.Backoff.RandomizationFactor == 0 {
		c.Backoff.RandomizationFactor = defaultRandomizationFactor
	}
	if c.Backoff.Multiplier == 0 {
		c.Backoff.Multiplier = defaultMultiplier
	}
	if c.Backoff.MaxInterval == 0 {
		c.Backoff.MaxInterval = defaultMaxInterval
	}
	if c.Backoff.MaxRetries == 0 {
		c.Backoff.MaxRetries = defaultMaxRetries
	}
	return c
}

// DependencyResolver returns a list of blob dependencies for a tag->digest mapping.
type DependencyResolver interface {
	Resolve(tag string, d core.Digest) (core.DigestList, error)
}

type subResolver struct {
	regexp   *regexp.Regexp
	resolver DependencyResolver
}

// Map is a DependencyResolver which maps tag patterns to sub resolvers.
type Map struct {
	subResolvers []*subResolver
}

// NewMap creates a new Map.
func NewMap(configs []Config, originClient blobclient.ClusterClient) (*Map, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("no config specified")
	}
	var subResolvers []*subResolver
	for _, config := range configs {
		config = config.applyDefaults()
		re, err := regexp.Compile(config.Namespace)
		if err != nil {
			return nil, fmt.Errorf("regexp: %s", err)
		}
		var sr *subResolver
		switch config.Type {
		case "docker":
			sr = &subResolver{re, &dockerResolver{originClient, config.Backoff}}
		case "default":
			sr = &subResolver{re, &defaultResolver{}}
		default:
			return nil, fmt.Errorf("type %s is undefined", config.Type)
		}
		subResolvers = append(subResolvers, sr)
	}
	return &Map{subResolvers}, nil
}

// Resolve executes the sub resolver configured for tag.
func (m *Map) Resolve(tag string, d core.Digest) (core.DigestList, error) {
	for _, sr := range m.subResolvers {
		if sr.regexp.MatchString(tag) {
			return sr.resolver.Resolve(tag, d)
		}
	}
	return nil, errNamespaceNotFound
}
