package blobserver

import (
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
)

type serverError struct {
	status int
	header http.Header
	msg    string
}

func serverErrorf(format string, args ...interface{}) *serverError {
	return &serverError{
		status: http.StatusInternalServerError,
		header: http.Header{},
		msg:    fmt.Sprintf(format, args...),
	}
}

func (e *serverError) Status(s int) *serverError {
	e.status = s
	return e
}

func (e *serverError) Header(k, v string) *serverError {
	e.header.Add(k, v)
	return e
}

func (e *serverError) Error() string {
	return fmt.Sprintf("server error %d: %s", e.status, e.msg)
}

func newBlobNotFoundError(d *image.Digest, err error) *serverError {
	return serverErrorf("cannot find blob data for digest %q: %s", d, err).
		Status(http.StatusNotFound)
}

func newUploadNotFoundError(uploadUUID string, err error) *serverError {
	return serverErrorf("cannot find upload %q: %s", uploadUUID, err).
		Status(http.StatusNotFound)
}
