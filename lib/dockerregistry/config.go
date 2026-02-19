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
package dockerregistry

import (
	"errors"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"
)

const (
	_rw = "rw"
	_ro = "ro"

	// defaultVerificationCacheSize is the default maximum number of entries
	// in the image verification LRU cache.
	defaultVerificationCacheSize = 300

	// defaultVerificationCacheTTL is the default duration a verified digest
	// stays in the cache before it expires.
	defaultVerificationCacheTTL = 5 * time.Minute
)

// VerificationCacheConfig defines configuration for the image verification
// LRU cache. The cache tracks recently verified digests to suppress duplicate
// metric and log emission on repeated verification of the same manifest.
//
// Image verification is always executed; the cache only controls whether
// associated logs/metrics are emitted again.
type VerificationCacheConfig struct {
	// Size is the maximum number of entries in the LRU cache.
	// If 0, defaults to 300.
	Size int `yaml:"size"`
	// TTL is how long a verified digest stays cached.
	// If 0, defaults to 5m.
	TTL time.Duration `yaml:"ttl"`
}

// applyDefaults fills zero-valued fields with default constants.
func (c VerificationCacheConfig) applyDefaults() VerificationCacheConfig {
	if c.Size == 0 {
		c.Size = defaultVerificationCacheSize
	}
	if c.TTL == 0 {
		c.TTL = defaultVerificationCacheTTL
	}
	return c
}

// validate checks that cache config values are valid after defaults are applied.
func (c VerificationCacheConfig) validate() error {
	if c.Size <= 0 {
		return errors.New("verification_cache.size must be > 0")
	}
	if c.TTL <= 0 {
		return errors.New("verification_cache.ttl must be a positive duration")
	}
	return nil
}

// Config defines registry configuration.
type Config struct {
	Docker            configuration.Configuration `yaml:"docker"`
	VerificationCache VerificationCacheConfig     `yaml:"verification_cache"`
}

// ReadWriteParameters builds parameters for a read-write driver.
func (c Config) ReadWriteParameters(
	transferer transfer.ImageTransferer,
	cas *store.CAStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"constructor": _rw,
		"config":      c,
		"transferer":  transferer,
		"castore":     cas,
		"metrics":     metrics,
	}
}

// ReadOnlyParameters builds parameters for a read-only driver.
func (c Config) ReadOnlyParameters(
	transferer transfer.ImageTransferer,
	bs BlobStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"constructor": _ro,
		"config":      c,
		"transferer":  transferer,
		"blobstore":   bs,
		"metrics":     metrics,
	}
}

// Build builds a new docker registry.
func (c Config) Build(parameters configuration.Parameters) (*registry.Registry, error) {
	c.Docker.Storage = configuration.Storage{
		Name: parameters,
		// Redirect is enabled by default in docker registry.
		// We implement redirect on proxy level so we do not need this in storage driver for now.
		"redirect": configuration.Parameters{
			"disable": true,
		},
	}
	return registry.NewRegistry(context.Background(), &c.Docker)
}
