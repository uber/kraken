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
package blobserver

import (
	"io"
	"net/http"
	"os"

	"github.com/docker/distribution/uuid"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/handler"
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
		return "", err
	} else if ok {
		return "", handler.ErrorStatus(http.StatusConflict)
	}
	uid = uuid.Generate().String()
	if err := u.cas.CreateUploadFile(uid, 0); err != nil {
		return "", handler.Errorf("create upload file: %s", err)
	}
	return uid, nil
}

func (u *uploader) patch(
	d core.Digest, uid string, chunk io.Reader, start, end int64) error {

	if ok, err := blobExists(u.cas, d); err != nil {
		return err
	} else if ok {
		return handler.ErrorStatus(http.StatusConflict)
	}
	f, err := u.cas.GetUploadFileReadWriter(uid)
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("get upload file: %s", err)
	}
	defer f.Close()
	if _, err := f.Seek(start, 0); err != nil {
		return handler.Errorf("seek offset %d: %s", start, err).Status(http.StatusBadRequest)
	}
	if _, err := io.CopyN(f, chunk, end-start); err != nil {
		return handler.Errorf("copy: %s", err)
	}
	return nil
}

func (u *uploader) commit(d core.Digest, uid string) error {
	if err := u.cas.MoveUploadFileToCache(uid, d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		if os.IsExist(err) {
			return handler.ErrorStatus(http.StatusConflict)
		}
		return handler.Errorf("move upload file to cache: %s", err)
	}
	return nil
}
