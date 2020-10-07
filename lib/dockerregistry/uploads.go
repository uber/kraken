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
	"bytes"
	"errors"
	"fmt"
	"io"
	stdpath "path"
	"time"

	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

type uploads interface {
	reader(path string, subtype PathSubType, offset int64) (io.ReadCloser, error)
	getContent(path string, subtype PathSubType) ([]byte, error)
	putContent(path string, subtype PathSubType, content []byte) error
	putBlobContent(path string, content []byte) error
	writer(path string, subtype PathSubType) (store.FileReadWriter, error)
	stat(path string) (storagedriver.FileInfo, error)
	list(path string, subtype PathSubType) ([]string, error)
	move(uploadPath, blobPath string) error
}

type casUploads struct {
	cas        *store.CAStore
	transferer transfer.ImageTransferer
}

func newCASUploads(cas *store.CAStore, transferer transfer.ImageTransferer) *casUploads {
	return &casUploads{cas, transferer}
}

func (u *casUploads) getContent(path string, subtype PathSubType) ([]byte, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	switch subtype {
	case _startedat:
		var s startedAtMetadata
		if err := u.cas.GetUploadFileMetadata(uuid, &s); err != nil {
			return nil, err
		}
		return s.Serialize()
	case _hashstates:
		algo, offset, err := GetUploadAlgoAndOffset(path)
		if err != nil {
			return nil, err
		}
		hs := newHashStateMetadata(algo, offset)
		if err := u.cas.GetUploadFileMetadata(uuid, hs); err != nil {
			return nil, err
		}
		return hs.Serialize()
	}
	return nil, InvalidRequestError{path}
}

func (u *casUploads) reader(path string, subtype PathSubType, offset int64) (io.ReadCloser, error) {
	switch subtype {
	case _data:
		uuid, err := GetUploadUUID(path)
		if err != nil {
			return nil, fmt.Errorf("get upload uuid: %s", err)
		}
		r, err := u.cas.GetUploadFileReader(uuid)
		if err != nil {
			return nil, fmt.Errorf("get reader: %w", err)
		}
		if _, err := r.Seek(offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek: %w", err)
		}
		return r, nil
	}
	return nil, InvalidRequestError{path}
}

func (u *casUploads) putContent(path string, subtype PathSubType, content []byte) error {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return err
	}
	switch subtype {
	case _startedat:
		if err := u.cas.CreateUploadFile(uuid, 0); err != nil {
			return fmt.Errorf("create upload file: %w", err)
		}
		s := newStartedAtMetadata(time.Now())
		if err := u.cas.SetUploadFileMetadata(uuid, s); err != nil {
			return fmt.Errorf("set started at: %w", err)
		}
		return nil
	case _hashstates:
		algo, offset, err := GetUploadAlgoAndOffset(path)
		if err != nil {
			return err
		}
		hs := newHashStateMetadata(algo, offset)
		if err := hs.Deserialize(content); err != nil {
			return fmt.Errorf("deserialize hash state: %s", err)
		}
		return u.cas.SetUploadFileMetadata(uuid, hs)
	}
	return InvalidRequestError{path}
}

func (u *casUploads) putBlobContent(path string, content []byte) error {
	d, err := GetBlobDigest(path)
	if err != nil {
		return fmt.Errorf("get digest: %s", err)
	}
	if err := u.cas.CreateCacheFile(d.Hex(), bytes.NewReader(content)); err != nil {
		return fmt.Errorf("create cache file: %w", err)
	}
	if err := u.transferer.Upload("TODO", d, store.NewBufferFileReader(content)); err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	return nil
}

func (u *casUploads) writer(path string, subtype PathSubType) (store.FileReadWriter, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	switch subtype {
	case _data:
		return u.cas.GetUploadFileReadWriter(uuid)
	}
	return nil, InvalidRequestError{path}
}

func (u *casUploads) stat(path string) (storagedriver.FileInfo, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	info, err := u.cas.GetUploadFileStat(uuid)
	if err != nil {
		return nil, err
	}
	// Hacking the path, since kraken storage driver is also the consumer of this info.
	// Instead of the relative path from root that docker registry expected, just use uuid.
	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    uuid,
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		},
	}, nil
}

func (u *casUploads) list(path string, subtype PathSubType) ([]string, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	switch subtype {
	case _hashstates:
		var paths []string
		u.cas.RangeUploadMetadata(uuid, func(md metadata.Metadata) error {
			if hs, ok := md.(*hashStateMetadata); ok {
				p := stdpath.Join("localstore", "_uploads", uuid, hs.dockerPath())
				paths = append(paths, p)
			}
			return nil
		})
		return paths, nil
	}
	return nil, InvalidRequestError{path}
}

func (u *casUploads) move(uploadPath, blobPath string) error {
	uuid, err := GetUploadUUID(uploadPath)
	if err != nil {
		return fmt.Errorf("get upload uuid: %s", err)
	}
	d, err := GetBlobDigest(blobPath)
	if err != nil {
		return fmt.Errorf("get blob uuid: %s", err)
	}
	if err := u.cas.MoveUploadFileToCache(uuid, d.Hex()); err != nil {
		return fmt.Errorf("move upload file to cache: %w", err)
	}
	f, err := u.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		return fmt.Errorf("get cache file: %w", err)
	}
	if err := u.transferer.Upload("TODO", d, f); err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	return nil
}

var errUploadsDisabled = errors.New("uploads are disabled")

type disabledUploads struct{}

func (u disabledUploads) reader(path string, subtype PathSubType, offset int64) (io.ReadCloser, error) {
	return nil, errUploadsDisabled
}

func (u disabledUploads) getContent(path string, subtype PathSubType) ([]byte, error) {
	return nil, errUploadsDisabled
}

func (u disabledUploads) putContent(path string, subtype PathSubType, content []byte) error {
	return errUploadsDisabled
}

func (u disabledUploads) putBlobContent(path string, content []byte) error {
	return errUploadsDisabled
}

func (u disabledUploads) writer(path string, subtype PathSubType) (store.FileReadWriter, error) {
	return nil, errUploadsDisabled
}

func (u disabledUploads) stat(path string) (storagedriver.FileInfo, error) {
	return nil, errUploadsDisabled
}

func (u disabledUploads) list(path string, subtype PathSubType) ([]string, error) {
	return nil, errUploadsDisabled
}

func (u disabledUploads) move(uploadPath, blobPath string) error {
	return errUploadsDisabled
}
