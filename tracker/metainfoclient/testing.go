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
package metainfoclient

import (
	"errors"
	"sync"

	"github.com/uber/kraken/core"
)

// TestClient is a thread-safe, in-memory client for simulating downloads.
type TestClient struct {
	sync.Mutex
	m map[core.Digest]*core.MetaInfo
}

// NewTestClient returns a new TestClient.
func NewTestClient() *TestClient {
	return &TestClient{m: make(map[core.Digest]*core.MetaInfo)}
}

// Upload "uploads" metainfo that can then be subsequently downloaded. Upload
// is not supported in the Client interface and exists soley for testing purposes.
func (c *TestClient) Upload(mi *core.MetaInfo) error {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.m[mi.Digest()]; ok {
		return errors.New("metainfo already exists")
	}
	c.m[mi.Digest()] = mi
	return nil
}

// Download returns the metainfo for digest. Ignores namespace.
func (c *TestClient) Download(namespace string, d core.Digest) (*core.MetaInfo, error) {
	c.Lock()
	defer c.Unlock()
	mi, ok := c.m[d]
	if !ok {
		return nil, ErrNotFound
	}
	return mi, nil
}
