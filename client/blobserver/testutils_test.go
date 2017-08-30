package blobserver

import (
	"bytes"
	"net/http"
)

const (
	emptyDigestHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	randomUUID     = "b9cb2c15-3cb5-46bf-a63c-26b0c5b9bc24"
)

type mockResponseWriter struct {
	header http.Header
	status int
	buf    *bytes.Buffer
}

func newMockResponseWriter() *mockResponseWriter {
	return &mockResponseWriter{
		header: http.Header{},
		buf:    bytes.NewBuffer([]byte("")),
	}
}
