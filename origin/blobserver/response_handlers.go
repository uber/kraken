package blobserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
)

const (
	uploadChunkSize int64 = 16 * 1024 * 1024 // 16MB
)

func downloadBlobHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "LocalStore not set")
	}

	// Get reader.
	blobReader, err := localStore.GetCacheFileReader(digest.Hex())
	if os.IsNotExist(err) {
		return nil, NewServerResponseWithError(http.StatusNotFound, "Cannot find blob data for digest: %s, error: %s", digest, err)
	} else if err != nil {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "Cannot read blob data for digest: %s, error: %s", digest, err)
	}
	defer blobReader.Close()

	// Read data.
	for {
		_, err := io.CopyN(writer, blobReader, uploadChunkSize)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, NewServerResponseWithError(http.StatusInternalServerError, "Cannot read digest: %s, error: %s", digest, err)
		}
	}

	return ctx, nil
}

func okHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusOK)

	return ctx, resp
}

func okOctetStreamHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusOK)
	writer.Header().Set("Content-Type", "application/octet-stream-v1")

	return ctx, resp
}

func acceptedHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusAccepted)
	writer.Header().Set("Content-Length", "0")

	return ctx, resp
}

func createdHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusCreated)
	writer.Header().Set("Content-Length", "0")

	return ctx, resp
}

func returnUploadLocationHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	if !ok {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "Digest not set")
	}

	resp := NewServerResponse(http.StatusAccepted)
	writer.Header().Set("Location", fmt.Sprintf("/blobs/uploads/%s", uploadUUID))
	writer.Header().Set("Content-Length", "0")

	return ctx, resp
}
