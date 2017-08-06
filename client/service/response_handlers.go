package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/client/dockerimage"
	"code.uber.internal/infra/kraken/client/store"
)

const (
	uploadChunkSize int64 = 16 * 1024 * 1024 // 16MB
)

func downloadBlobHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerError) {
	digest, ok := ctx.Value(ctxKeyDigest).(*dockerimage.Digest)
	if !ok {
		return nil, NewServerError(http.StatusInternalServerError, "Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerError(http.StatusInternalServerError, "LocalStore not set")
	}

	// Get reader.
	blobReader, err := localStore.GetCacheFileReader(digest.Hex())
	if os.IsNotExist(err) {
		return nil, NewServerError(http.StatusNotFound, "Cannot find blob data for digest: %s, error: %s", digest, err)
	} else if err != nil {
		return nil, NewServerError(http.StatusInternalServerError, "Cannot read blob data for digest: %s, error: %s", digest, err)
	}
	defer blobReader.Close()

	// Read data.
	for {
		_, err := io.CopyN(writer, blobReader, uploadChunkSize)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, NewServerError(http.StatusInternalServerError, "Cannot read digest: %s, error: %s", digest, err)
		}
	}

	return ctx, nil
}

func okOctetStreamHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerError) {
	writer.Header().Set("Content-Type", "application/octet-stream-v1")
	writer.WriteHeader(http.StatusOK)

	return ctx, nil
}

func returnUploadLocationHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerError) {
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	if !ok {
		return nil, NewServerError(http.StatusInternalServerError, "Digest not set")
	}

	writer.Header().Set("Location", fmt.Sprintf("/blobs/uploads/%s", uploadUUID))
	writer.Header().Set("Content-Length", "0")
	writer.WriteHeader(http.StatusAccepted)

	return ctx, nil
}
