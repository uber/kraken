package blobserver

import (
	"io"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/handler"
	"github.com/docker/distribution/uuid"
)

// uploader executes a chunked upload.
type uploader struct {
	fs store.OriginFileStore
}

func newUploader(fs store.OriginFileStore) *uploader {
	return &uploader{fs}
}

func (u *uploader) start(d core.Digest) (uid string, err error) {
	if ok, err := blobExists(u.fs, d); err != nil {
		return "", err
	} else if ok {
		return "", handler.ErrorStatus(http.StatusConflict)
	}
	uid = uuid.Generate().String()
	if err := u.fs.CreateUploadFile(uid, 0); err != nil {
		return "", handler.Errorf("create upload file: %s", err)
	}
	return uid, nil
}

func (u *uploader) patch(
	d core.Digest, uid string, chunk io.Reader, start, end int64) error {

	if ok, err := blobExists(u.fs, d); err != nil {
		return err
	} else if ok {
		return handler.ErrorStatus(http.StatusConflict)
	}
	f, err := u.fs.GetUploadFileReadWriter(uid)
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

func (u *uploader) verify(d core.Digest, uid string) error {
	digester := core.NewDigester()
	f, err := u.fs.GetUploadFileReader(uid)
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("get upload file: %s", err)
	}
	defer f.Close()
	computedDigest, err := digester.FromReader(f)
	if err != nil {
		return handler.Errorf("calculate digest: %s", err)
	}
	if computedDigest != d {
		return handler.
			Errorf("computed digest %s doesn't match parameter %s", computedDigest, d).
			Status(http.StatusBadRequest)
	}
	return nil
}

func (u *uploader) commit(d core.Digest, uid string) error {
	if err := u.fs.MoveUploadFileToCache(uid, d.Hex()); err != nil {
		if os.IsExist(err) {
			return handler.Errorf("digest already exists").Status(http.StatusConflict)
		}
		return handler.Errorf("move upload file to cache: %s", err)
	}
	return nil
}
