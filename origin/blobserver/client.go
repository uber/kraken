package blobserver

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
)

// RedirectError occurs when a Client is scoped to the wrong origin.
type RedirectError struct {
	Locations []string
}

func newRedirectError(h http.Header) RedirectError {
	locations := strings.Split(h.Get("Origin-Locations"), ",")
	return RedirectError{locations}
}

func (e RedirectError) Error() string {
	return fmt.Sprintf("incorrect origin, must redirect to: %v", e.Locations)
}

// Client provides a wrapper around all Server HTTP endpoints.
type Client interface {
	CheckBlob(d image.Digest) error
	GetBlob(d image.Digest) (io.Reader, error)
	DeleteBlob(d image.Digest) error
	UploadBlob(d image.Digest) (uuid string, err error)
	PatchUpload(d image.Digest, uuid string, start, stop int64, r io.Reader) error
	CommitUpload(d image.Digest, uuid string) error
	Repair() (io.Reader, error)
}
