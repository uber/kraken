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
	"bytes"
	"context"
	"fmt"

	"github.com/cenkalti/backoff"
	"github.com/docker/distribution"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"
)

type dockerResolver struct {
	originClient  blobclient.ClusterClient
	backoffConfig httputil.ExponentialBackOffConfig
}

// Resolve returns all layers + manifest of given tag as its dependencies.
func (r *dockerResolver) Resolve(tag string, d core.Digest) (core.DigestList, error) {
	m, err := r.downloadManifest(tag, d)
	if err != nil {
		return nil, fmt.Errorf("download manifest: %w", err)
	}
	deps, err := dockerutil.GetManifestReferences(m)
	if err != nil {
		return nil, fmt.Errorf("get manifest references: %w", err)
	}
	return append(deps, d), nil
}

func (r *dockerResolver) downloadManifest(tag string, d core.Digest) (distribution.Manifest, error) {
	buf := &bytes.Buffer{}
	attempt := 0

	retryFunc := func() error {
		attempt++
		buf.Reset()

		err := r.originClient.DownloadBlob(context.Background(), tag, d, buf)
		if err == nil {
			return nil
		}

		if attempt > 1 {
			log.With("tag", tag, "digest", d.Hex(), "attempt", attempt, "error", err).
				Warn("Manifest download failed, will retry")
		}

		if err != blobclient.ErrBlobNotFound &&
			!httputil.IsNetworkError(err) {
			return backoff.Permanent(err)
		}

		return err
	}

	if err := backoff.Retry(retryFunc, r.backoffConfig.Build()); err != nil {
		return nil, err
	}
	manifest, _, err := dockerutil.ParseManifest(buf)
	if err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return manifest, nil
}
