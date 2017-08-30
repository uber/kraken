package blobserver

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"code.uber.internal/infra/kraken/client/dockerimage"
	"code.uber.internal/infra/kraken/client/store"

	"github.com/stretchr/testify/require"
)

func (r *mockResponseWriter) Header() http.Header {
	return r.header
}

func (r *mockResponseWriter) Write(buf []byte) (int, error) {
	return r.buf.Write(buf)
}

func (r *mockResponseWriter) WriteHeader(status int) {
	r.status = status
}

func TestDownloadBlobHandlerValid(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalStoreFixture()
	defer cleanup()
	localStore.CreateUploadFile(randomUUID, 0)
	writer, _ := localStore.GetUploadFileReadWriter(randomUUID)
	writer.Write([]byte("Hello world!!!"))
	contentDigest, _ := dockerimage.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
	localStore.MoveUploadFileToCache(randomUUID, contentDigest.Hex())
	ctx := context.WithValue(context.Background(), ctxKeyDigest, contentDigest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mockWriter := newMockResponseWriter()
	ctx, se := downloadBlobHandler(ctx, mockWriter)
	require.Nil(se)
	require.Equal(mockWriter.buf.Bytes()[:len("Hello world!!!")], []byte("Hello world!!!"))
}

func TestDownloadBlobHandlerInvalid(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalStoreFixture()
	defer cleanup()
	localStore.CreateUploadFile(randomUUID, 0)
	writer, _ := localStore.GetUploadFileReadWriter(randomUUID)
	writer.Write([]byte("Hello world!!!"))
	contentDigest, _ := dockerimage.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
	localStore.MoveUploadFileToCache(randomUUID, contentDigest.Hex())
	wrongDigest, _ := dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx := context.WithValue(context.Background(), ctxKeyDigest, wrongDigest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mockWriter := newMockResponseWriter()
	ctx, se := downloadBlobHandler(ctx, mockWriter)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.GetStatusCode(), http.StatusNotFound)
}
