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
	"errors"
	"io"
	"net/http"
	"os"

	"github.com/docker/distribution/uuid"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/log"
)

// uploader executes a chunked upload.
type uploader struct {
	store *disk.Store
}

func newUploader(store *disk.Store) *uploader {
	return &uploader{store}
}

func (u *uploader) start(d core.Digest, size uint64) (uid string, err error) {
	uid = uuid.Generate().String()
	f, err := u.store.Create(d.Hex(), size)
	if errors.Is(err, os.ErrExist) {
		log.With("digest", d.Hex()).Debug("Blob already exists, cannot start new upload")
		return "", handler.ErrorStatus(http.StatusConflict)
	}
	if err != nil {
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to create file: %s", err)
		return "", handler.Errorf("create file: %s", err)
	}
	closers.Close(f)
	log.With("digest", d.Hex(), "uid", uid).Debug("Created file")
	return uid, nil
}

func (u *uploader) patch(
	d core.Digest, uid string, chunk io.Reader, start, end int64,
) error {
	_, ok := u.store.ScopeComplete().Has(d.Hex())
	if ok {
		log.With("digest", d.Hex(), "uid", uid).Debug("Blob already exists and is complete, cannot patch upload")
		return handler.ErrorStatus(http.StatusConflict)
	}
	f, err := u.store.ScopeIncomplete().Open(d.Hex())
	if os.IsNotExist(err) {
		log.With("digest", d.Hex(), "uid", uid).Warn("Incomplete file not found")
		return handler.ErrorStatus(http.StatusNotFound)
	}
	if err != nil {
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to get upload file: %s", err)
		return handler.Errorf("get file: %s", err)
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
	log.With("digest", d.Hex(), "uid", uid).Debug("Marking file as complete")
	err := u.store.MarkComplete(d.Hex())
	if os.IsNotExist(err) {
		log.With("digest", d.Hex(), "uid", uid).Warn("File not found during commit")
		return handler.ErrorStatus(http.StatusNotFound)
	}
	if err != nil {
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to mark file as complete: %s", err)
		return handler.Errorf("mark file as complete: %s", err)
	}
	log.With("digest", d.Hex(), "uid", uid).Info("Successfully marked file as complete")
	return nil
}
