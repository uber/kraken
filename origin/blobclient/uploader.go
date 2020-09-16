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
package blobclient

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/httputil"
)

// uploader provides methods for executing a chunked upload.
type uploader interface {
	start(d core.Digest) (uid string, err error)
	patch(d core.Digest, uid string, start, stop int64, chunk io.Reader) error
	commit(d core.Digest, uid string) error
}

func runChunkedUpload(u uploader, d core.Digest, blob io.Reader, chunkSize int64) error {
	if err := runChunkedUploadHelper(u, d, blob, chunkSize); err != nil && !httputil.IsConflict(err) {
		return err
	}
	return nil
}

func runChunkedUploadHelper(u uploader, d core.Digest, blob io.Reader, chunkSize int64) error {
	uid, err := u.start(d)
	if err != nil {
		return err
	}
	var pos int64
	buf := make([]byte, chunkSize)
	for {
		n, err := blob.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read blob: %s", err)
		}
		chunk := bytes.NewReader(buf[:n])
		stop := pos + int64(n)
		if err := u.patch(d, uid, pos, stop, chunk); err != nil {
			return err
		}
		pos = stop
	}
	return u.commit(d, uid)
}

// transferClient executes chunked uploads for internal blob transfers.
type transferClient struct {
	addr string
	tls  *tls.Config
}

func newTransferClient(addr string, tls *tls.Config) *transferClient {
	return &transferClient{addr, tls}
}

func (c *transferClient) start(d core.Digest) (uid string, err error) {
	r, err := httputil.Post(
		fmt.Sprintf("http://%s/internal/blobs/%s/uploads", c.addr, d),
		httputil.SendTLS(c.tls))
	if err != nil {
		return "", err
	}
	uid = r.Header.Get("Location")
	if uid == "" {
		return "", errors.New("request succeeded, but Location header not set")
	}
	return uid, nil
}

func (c *transferClient) patch(
	d core.Digest, uid string, start, stop int64, chunk io.Reader) error {

	_, err := httputil.Patch(
		fmt.Sprintf("http://%s/internal/blobs/%s/uploads/%s", c.addr, d, uid),
		httputil.SendBody(chunk),
		httputil.SendHeaders(map[string]string{
			"Content-Range": fmt.Sprintf("%d-%d", start, stop),
		}),
		httputil.SendTLS(c.tls))
	return err
}

func (c *transferClient) commit(d core.Digest, uid string) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/internal/blobs/%s/uploads/%s", c.addr, d, uid),
		httputil.SendTimeout(15*time.Minute),
		httputil.SendTLS(c.tls))
	return err
}

type uploadType int

const (
	_publicUpload = iota + 1
	_duplicateUpload
)

// uploadClient executes chunked uploads for external cluster upload operations.
type uploadClient struct {
	addr       string
	namespace  string
	uploadType uploadType
	delay      time.Duration
	tls        *tls.Config
}

func newUploadClient(
	addr string, namespace string, t uploadType, delay time.Duration, tls *tls.Config) *uploadClient {

	return &uploadClient{addr, namespace, t, delay, tls}
}

func (c *uploadClient) start(d core.Digest) (uid string, err error) {
	r, err := httputil.Post(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/uploads",
			c.addr, url.PathEscape(c.namespace), d),
		httputil.SendTLS(c.tls))
	if err != nil {
		return "", err
	}
	uid = r.Header.Get("Location")
	if uid == "" {
		return "", errors.New("request succeeded, but Location header not set")
	}
	return uid, nil
}

func (c *uploadClient) patch(
	d core.Digest, uid string, start, stop int64, chunk io.Reader) error {

	_, err := httputil.Patch(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/uploads/%s",
			c.addr, url.PathEscape(c.namespace), d, uid),
		httputil.SendBody(chunk),
		httputil.SendHeaders(map[string]string{
			"Content-Range": fmt.Sprintf("%d-%d", start, stop),
		}),
		httputil.SendTLS(c.tls))
	return err
}

// DuplicateCommitUploadRequest defines HTTP request body.
type DuplicateCommitUploadRequest struct {
	Delay time.Duration `yaml:"delay"`
}

func (c *uploadClient) commit(d core.Digest, uid string) error {
	var template string
	var body io.Reader
	switch c.uploadType {
	case _publicUpload:
		template = "http://%s/namespace/%s/blobs/%s/uploads/%s"
	case _duplicateUpload:
		template = "http://%s/internal/duplicate/namespace/%s/blobs/%s/uploads/%s"
		b, err := json.Marshal(DuplicateCommitUploadRequest{c.delay})
		if err != nil {
			return fmt.Errorf("json: %s", err)
		}
		body = bytes.NewBuffer(b)
	default:
		return fmt.Errorf("unknown upload type: %d", c.uploadType)
	}
	_, err := httputil.Put(
		fmt.Sprintf(template, c.addr, url.PathEscape(c.namespace), d, uid),
		httputil.SendTimeout(15*time.Minute),
		httputil.SendBody(body),
		httputil.SendTLS(c.tls))
	return err
}
