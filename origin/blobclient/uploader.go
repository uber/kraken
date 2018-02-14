package blobclient

import (
	"errors"
	"fmt"
	"io"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// uploader provides methods for executing a chunked upload.
type uploader interface {
	start(d core.Digest) (uid string, err error)
	patch(d core.Digest, uid string, start, stop int64, chunk io.Reader) error
	commit(d core.Digest, uid string) error
}

func runChunkedUpload(u uploader, d core.Digest, blob io.Reader, size int64, chunkSize int64) error {
	uid, err := u.start(d)
	if err != nil {
		if httputil.IsConflict(err) {
			return nil
		}
		return err
	}
	var start int64
	for start < size {
		n := min(chunkSize, size-start)
		chunk := io.LimitReader(blob, n)
		if err := u.patch(d, uid, start, start+n, chunk); err != nil {
			return err
		}
		start += n
	}
	return u.commit(d, uid)
}

// transferClient executes chunked uploads for internal blob transfers.
type transferClient struct {
	addr string
}

func newTransferClient(addr string) *transferClient {
	return &transferClient{addr}
}

func (c *transferClient) start(d core.Digest) (uid string, err error) {
	r, err := httputil.Post(fmt.Sprintf("http://%s/internal/blobs/%s/uploads", c.addr, d))
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
		}))
	return err
}

func (c *transferClient) commit(d core.Digest, uid string) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/internal/blobs/%s/uploads/%s", c.addr, d, uid),
		httputil.SendTimeout(15*time.Minute))
	return err
}

// uploadClient executes chunked uploads for external cluster upload operations.
type uploadClient struct {
	addr      string
	namespace string
	through   bool
}

func newUploadClient(addr string, namespace string, through bool) *uploadClient {
	return &uploadClient{addr, namespace, through}
}

func (c *uploadClient) start(d core.Digest) (uid string, err error) {
	r, err := httputil.Post(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/uploads", c.addr, c.namespace, d))
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
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/uploads/%s", c.addr, c.namespace, d, uid),
		httputil.SendBody(chunk),
		httputil.SendHeaders(map[string]string{
			"Content-Range": fmt.Sprintf("%d-%d", start, stop),
		}))
	return err
}

func (c *uploadClient) commit(d core.Digest, uid string) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/uploads/%s?through=%t",
			c.addr, c.namespace, d, uid, c.through),
		httputil.SendTimeout(15*time.Minute))
	return err
}
