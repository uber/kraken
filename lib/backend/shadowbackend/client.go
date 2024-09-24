// Copyright (c) 2016-2020 Uber Technologies, Inc.
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
package shadowbackend

import (
	"errors"
	"fmt"
	"io"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/hdfsbackend"
	"github.com/uber/kraken/lib/backend/s3backend"
	"github.com/uber/kraken/lib/backend/sqlbackend"
	"github.com/uber/kraken/lib/backend/testfs"
	"github.com/uber/kraken/utils/log"
	"gopkg.in/yaml.v2"
)

const _shadow = "shadow"

func init() {
	backend.Register(_shadow, &factory{})
}

type factory struct{}

func (f *factory) Name() string {
	return _shadow
}

func (f *factory) Create(
	confRaw interface{}, masterAuthConfig backend.AuthConfig, stats tally.Scope) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, fmt.Errorf("marshal shadow config: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, fmt.Errorf("unmarshal shadow config: %v", err)
	}
	return NewClient(config, masterAuthConfig, stats)
}

// Client implements a backend.Client for shadow mode. See the README for full details on what shadow mode means.
type Client struct {
	cfg    Config
	active backend.Client
	shadow backend.Client
	stats  tally.Scope
}

// Option allows setting optional Client parameters.
type Option func(*Client)

// NewClient creates a new shadow Client
func NewClient(config Config, masterAuthConfig backend.AuthConfig, stats tally.Scope) (*Client, error) {
	aAuthConfig, sAuthConfig, err := extractAuthConfigs(config, masterAuthConfig)
	if err != nil {
		return nil, err
	}
	a, err := getBackendClient(config.ActiveClientConfig, aAuthConfig, stats)
	if err != nil {
		return nil, err
	}

	s, err := getBackendClient(config.ShadowClientConfig, sAuthConfig, stats)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:    config,
		active: a,
		shadow: s,
		stats:  stats,
	}, nil
}

func extractAuthConfigs(config Config, masterAuthConfig backend.AuthConfig) (interface{}, interface{}, error) {
	aName, sName, err := getBackendNames(config)
	if err != nil {
		return nil, nil, err
	}

	aAuth, ok := masterAuthConfig[aName]
	if !ok {
		return nil, nil, fmt.Errorf("active backend auth config missing")
	}
	sAuth, ok := masterAuthConfig[sName]
	if !ok {
		return nil, nil, fmt.Errorf("shadow backend auth config missing")
	}
	return aAuth, sAuth, nil
}

func getBackendNames(config Config) (string, string, error) {
	if len(config.ActiveClientConfig) != 1 {
		return "", "", fmt.Errorf("no active backend or more than one active backend configured")
	}
	if len(config.ShadowClientConfig) != 1 {
		return "", "", fmt.Errorf("no shadow backend or more than one shadow backend configured")
	}
	var aName string
	var sName string
	for aName = range config.ActiveClientConfig { // Map should have only 1 key/value
	}
	for sName = range config.ShadowClientConfig { // Map should have only 1 key/value
	}
	return aName, sName, nil
}

func getBackendClient(backendConfig map[string]interface{}, authConfRaw interface{}, stats tally.Scope) (backend.Client, error) {
	var name string
	var confRaw interface{}

	// TODO Re-implementing all the factory functions is bad form, but because backends.getFactory isn't public there
	// is no way to access them currently. Opened https://github.com/uber/kraken/issues/213 to address this.
	for name, confRaw = range backendConfig {
		switch name {
		case "sql":
			confBytes, err := yaml.Marshal(confRaw)
			if err != nil {
				return nil, fmt.Errorf("marshal sql config: %s", err)
			}
			var config sqlbackend.Config
			if err := yaml.Unmarshal(confBytes, &config); err != nil {
				return nil, fmt.Errorf("unmarshal sql config: %s", err)
			}
			authConfBytes, err := yaml.Marshal(authConfRaw)
			var userAuth sqlbackend.UserAuthConfig
			if err := yaml.Unmarshal(authConfBytes, &userAuth); err != nil {
				return nil, fmt.Errorf("unmarshal sql auth config: %s", err)
			}
			return sqlbackend.NewClient(config, userAuth, stats)
		case "hdfs":
			confBytes, err := yaml.Marshal(confRaw)
			if err != nil {
				return nil, fmt.Errorf("marshal hdfs config: %s", err)
			}

			var config hdfsbackend.Config
			if err := yaml.Unmarshal(confBytes, &config); err != nil {
				return nil, fmt.Errorf("unmarshal hdfs config: %s", err)
			}

			return hdfsbackend.NewClient(config, stats)
		case "s3":
			confBytes, err := yaml.Marshal(confRaw)
			if err != nil {
				return nil, fmt.Errorf("marshal s3 config: %s", err)
			}

			var config s3backend.Config
			if err := yaml.Unmarshal(confBytes, &config); err != nil {
				return nil, fmt.Errorf("unmarshal s3 config: %s", err)
			}
			authConfBytes, err := yaml.Marshal(authConfRaw)
			var userAuth s3backend.UserAuthConfig
			if err := yaml.Unmarshal(authConfBytes, &userAuth); err != nil {
				return nil, fmt.Errorf("unmarshal s3 auth config: %s", err)
			}

			return s3backend.NewClient(config, userAuth, stats)
		case "testfs":
			confBytes, err := yaml.Marshal(confRaw)
			if err != nil {
				return nil, fmt.Errorf("marshal testfs config: %s", err)
			}

			var config testfs.Config
			if err := yaml.Unmarshal(confBytes, &config); err != nil {
				return nil, fmt.Errorf("unmarshal testfs config: %s", err)
			}

			return testfs.NewClient(config, stats)
		default:
			return nil, fmt.Errorf("unsupported backend type '%s'", name)
		}
	}

	return nil, nil
}

func isNotFoundErr(err error) bool {
	return err != nil && err == backenderrors.ErrBlobNotFound
}

// Stat returns a non-nil core.BlobInfo struct if the data exists, an error otherwise.
func (c *Client) Stat(namespace string, name string) (*core.BlobInfo, error) {
	// read from both, fail if error from either
	res, errA := c.active.Stat(namespace, name)
	_, errS := c.shadow.Stat(namespace, name)

	if isNotFoundErr(errA) && isNotFoundErr(errS) {
		return nil, backenderrors.ErrBlobNotFound
	}

	if errA != nil || errS != nil {
		if errA != nil && errS == nil {
			log.Errorf("[Stat] error getting %s for namespace '%s' from active backend: %v", name, namespace, errA)
			return nil, errA
		}

		if errS != nil && errA == nil {
			log.Errorf("[Stat] error getting %s for namespace '%s' from shadow backend: %v", name, namespace, errS)
			return nil, errS
		}

		return nil, fmt.Errorf("[Stat] error in both backends for %s in namespace '%s'. active: '%v', shadow: '%v'", name, namespace, errA, errS)
	}

	return res, nil
}

// Download gets the data from the backend and then writes it to the output writer.
func (c *Client) Download(namespace string, name string, dst io.Writer) error {
	err := c.active.Download(namespace, name, dst)
	return err
}

// Upload upserts the data into the backend.
func (c *Client) Upload(namespace string, name string, src io.Reader) error {
	rs, ok := src.(io.ReadSeeker)
	if !ok {
		return errors.New("refusing upload: src does not implement io.Seeker")
	}

	// write to both, fail if write fails for any
	err := c.active.Upload(namespace, name, rs)
	if err != nil {
		return err
	}

	// Need to rewind the ReadSeeker here before the second upload
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return err
	}

	err = c.shadow.Upload(namespace, name, rs)
	if err != nil {
		return err
	}

	return nil
}

// List lists names with start with prefix.
func (c *Client) List(prefix string, opts ...backend.ListOption) (*backend.ListResult, error) {
	res, err := c.active.List(prefix, opts...)
	if err != nil {
		return nil, err
	}
	return res, nil
}
