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
	"io"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
)

// NoopNamespace is a special namespace which always returns a NoopClient.
const NoopNamespace = "__noop__"

// NoopClient is a special Client whose uploads always succeeds and whose blob
// lookups always 404. It is useful for users who want to operate on blobs that
// will be temporarily stored in the origin cluster and not backed up in remote
// storage.
type NoopClient struct{}

// Stat always returns ErrBlobNotFound.
func (c NoopClient) Stat(namespace, name string) (*core.BlobInfo, error) {
	return nil, backenderrors.ErrBlobNotFound
}

// Upload always returns nil.
func (c NoopClient) Upload(namespace, name string, src io.Reader) error {
	return nil
}

// Download always returns ErrBlobNotFound.
func (c NoopClient) Download(namespace, name string, dst io.Writer) error {
	return backenderrors.ErrBlobNotFound
}

// List always returns nil.
func (c NoopClient) List(prefix string, opts ...ListOption) (*ListResult, error) {
	return nil, nil
}
