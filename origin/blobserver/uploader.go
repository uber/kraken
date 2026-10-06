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
package blobserver

import (
	"io"
	"net/http"
	"os"

	"github.com/docker/distribution/uuid"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/log"
)

// uploader executes a chunked upload.
type uploader struct {
	cas *store.CAStore
}

func newUploader(cas *store.CAStore) *uploader {
	return &uploader{cas}
}

func (u *uploader) start(d core.Digest) (uid string, err error) {
	if ok, err := blobExists(u.cas, d); err != nil {
		log.With("digest", d.Hex()).Errorf("Failed to check if blob exists: %s", err)
		return "", err
	} else if ok {
		log.With("digest", d.Hex()).Debug("Blob already exists, cannot start new upload")
		return "", handler.ErrorStatus(http.StatusConflict)
	}
	uid = uuid.Generate().String()
	if err := u.cas.CreateUploadFile(uid, 0); err != nil {
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to create upload file: %s", err)
		return "", handler.Errorf("create upload file: %s", err)
	}
	log.With("digest", d.Hex(), "uid", uid).Debug("Created upload file")
	return uid, nil
}

func (u *uploader) patch(
	d core.Digest, uid string, chunk io.Reader, start, end int64,
) error {
	if ok, err := blobExists(u.cas, d); err != nil {
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to check if blob exists: %s", err)
		return err
	} else if ok {
		log.With("digest", d.Hex(), "uid", uid).Debug("Blob already exists, cannot patch upload")
		return handler.ErrorStatus(http.StatusConflict)
	}
	f, err := u.cas.GetUploadFileReadWriter(uid)
	if err != nil {
		if os.IsNotExist(err) {
			log.With("digest", d.Hex(), "uid", uid).Warn("Upload file not found")
			return handler.ErrorStatus(http.StatusNotFound)
		}
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to get upload file: %s", err)
		return handler.Errorf("get upload file: %s", err)
	}
	defer closers.Close(f)
	if _, err := f.Seek(start, 0); err != nil {
		log.With("digest", d.Hex(), "uid", uid, "offset", start).Errorf("Failed to seek to offset: %s", err)
		return handler.Errorf("seek offset %d: %s", start, err).Status(http.StatusBadRequest)
	}
	chunkSize := end - start
	if _, err := io.CopyN(f, chunk, chunkSize); err != nil {
		log.With("digest", d.Hex(), "uid", uid, "start", start, "end", end, "chunk_size", chunkSize).Errorf("Failed to copy chunk data: %s", err)
		return handler.Errorf("copy: %s", err)
	}
	log.With("digest", d.Hex(), "uid", uid, "start", start, "end", end, "chunk_size", chunkSize).Debug("Successfully patched upload chunk")
	return nil
}

func (u *uploader) commit(d core.Digest, uid string) error {
	log.With("digest", d.Hex(), "uid", uid).Debug("Moving upload file to cache")
	if err := u.cas.MoveUploadFileToCache(uid, d.Hex()); err != nil {
		if os.IsNotExist(err) {
			log.With("digest", d.Hex(), "uid", uid).Warn("Upload file not found during commit")
			return handler.ErrorStatus(http.StatusNotFound)
		}
		if os.IsExist(err) {
			log.With("digest", d.Hex(), "uid", uid).Debug("Blob already exists in cache")
			return handler.ErrorStatus(http.StatusConflict)
		}
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to move upload file to cache: %s", err)
		return handler.Errorf("move upload file to cache: %s", err)
	}
	log.With("digest", d.Hex(), "uid", uid).Info("Successfully committed upload to cache")
	return nil
}
