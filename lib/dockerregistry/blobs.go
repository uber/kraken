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
package dockerregistry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

// BlobStore defines cache file accessors.
type BlobStore interface {
	GetCacheFileStat(name string) (os.FileInfo, error)
	GetCacheFileReader(name string) (store.FileReader, error)
}

type blobs struct {
	bs         BlobStore
	transferer transfer.ImageTransferer
}

func newBlobs(bs BlobStore, transferer transfer.ImageTransferer) *blobs {
	return &blobs{bs, transferer}
}

// getDigest returns blob digest given a blob path.
func (b *blobs) getDigest(path string) ([]byte, error) {
	digest, err := GetLayerDigest(path)
	if err != nil {
		return nil, err
	}

	return []byte(digest.String()), nil
}

func (b *blobs) stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	repo, err := parseRepo(ctx)
	if err != nil {
		return nil, fmt.Errorf("parse repo %s: %s", path, err)
	}
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	bi, err := b.transferer.Stat(repo, digest)
	if err != nil {
		return nil, fmt.Errorf("transferer stat: %w", err)
	}
	// Hacking the path, since kraken storage driver is also the consumer of this info.
	// Instead of the relative path from root that docker registry expected, just use content hash.
	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    digest.Hex(),
			Size:    bi.Size,
			ModTime: time.Now(),
			IsDir:   false,
		},
	}, nil
}

func (b *blobs) reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return b.getCacheReaderHelper(ctx, path, offset)
}

func (b *blobs) getContent(ctx context.Context, path string) ([]byte, error) {
	r, err := b.getCacheReaderHelper(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func (b *blobs) getCacheReaderHelper(
	ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	repo, err := parseRepo(ctx)
	if err != nil {
		return nil, fmt.Errorf("parse repo %s: %s", path, err)
	}

	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, fmt.Errorf("get layer digest %s: %s", path, err)
	}

	r, err := b.transferer.Download(repo, digest)
	if err != nil {
		return nil, fmt.Errorf("transferer download: %w", err)
	}

	if _, err := r.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("seek: %s", err)
	}
	return r, nil
}

func parseRepo(ctx context.Context) (string, error) {
	repo, ok := ctx.Value("vars.name").(string)
	if !ok {
		return "", errors.New("could not parse vars.name from context")
	}
	if repo == "" {
		return "", errors.New("vars.name is empty")
	}
	return repo, nil
}
