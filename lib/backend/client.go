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
	"fmt"
	"io"

	"github.com/uber/kraken/core"
)

var _factories = make(map[string]ClientFactory)

// ClientFactory creates backend client given name.
type ClientFactory interface {
	Create(config interface{}, authConfig interface{}) (Client, error)
}

// Register registers new Factory with corresponding backend client name.
func Register(name string, factory ClientFactory) {
	_factories[name] = factory
}

// GetFactory returns backend client factory given client name.
// This function should stay public to allow for wrapper custom backends.
func GetFactory(name string) (ClientFactory, error) {
	factory, ok := _factories[name]
	if !ok {
		return nil, fmt.Errorf("no backend client defined with name %s", name)
	}
	return factory, nil
}

// Client defines an interface for accessing blobs on a remote storage backend.
//
// Implementations of Client must be thread-safe, since they are cached and
// used concurrently by Manager.
type Client interface {
	// Stat returns blob info for name. All implementations should return
	// backenderrors.ErrBlobNotFound when the blob was not found.
	//
	// Stat is useful when we need to quickly know if a blob exists (and maybe
	// some basic information about it), without downloading the entire blob,
	// which may be very large.
	Stat(namespace, name string) (*core.BlobInfo, error)

	// Upload uploads src into name.
	Upload(namespace, name string, src io.Reader) error

	// Download downloads name into dst. All implementations should return
	// backenderrors.ErrBlobNotFound when the blob was not found.
	Download(namespace, name string, dst io.Writer) error

	// List lists entries whose names start with prefix.
	List(prefix string, opts ...ListOption) (*ListResult, error)
}
