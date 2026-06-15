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

// Package metainfosidecar stores a blob's serialized metainfo as a small
// backend object next to the blob. A cold origin fetches it to learn the real
// piece sums without downloading the whole blob, so it can lazily range-fetch
// and verify individual pieces.
package metainfosidecar

import (
	"bytes"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
)

// Suffix is appended to a blob name to form its metainfo sidecar name.
const Suffix = ".kmeta"

// Name returns the sidecar name for the given blob name.
func Name(name string) string {
	return name + Suffix
}

// Fetch downloads and deserializes the metainfo sidecar for d from the backend.
// Returns backenderrors.ErrBlobNotFound when the sidecar does not exist.
func Fetch(c backend.Client, namespace string, d core.Digest) (*core.MetaInfo, error) {
	var buf bytes.Buffer
	if err := c.Download(namespace, Name(d.Hex()), &buf); err != nil {
		return nil, err
	}
	return core.DeserializeMetaInfo(buf.Bytes())
}
