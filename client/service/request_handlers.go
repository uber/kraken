package service

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"code.uber.internal/infra/kraken/client/dockerimage"
	"code.uber.internal/infra/kraken/client/store"

	"github.com/docker/distribution/uuid"
	"github.com/pressly/chi"
)

func parseDigestHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	digestParam := chi.URLParam(request, "digest")
	if len(digestParam) == 0 {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Failed to parse an empty digest")
	}

	var err error
	digestRaw, err := url.QueryUnescape(digestParam)
	if err != nil {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot unescape digest: %s, error: %s", digestParam, err)
	}
	digest, err := dockerimage.NewDigestFromString(digestRaw)
	if err != nil {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot parse digest: %s, error: %s", digestRaw, err)
	}
	return context.WithValue(ctx, ctxKeyDigest, digest), nil
}

func parseDigestFromQueryHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	digestParam := request.URL.Query().Get("digest")
	if len(digestParam) == 0 {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Failed to parse an empty digest")
	}

	var err error
	digestRaw, err := url.QueryUnescape(digestParam)
	if err != nil {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot unescape digest: %s, error: %s", digestParam, err)
	}
	digest, err := dockerimage.NewDigestFromString(digestRaw)
	if err != nil {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot parse digest: %s, error: %s", digestRaw, err)
	}
	return context.WithValue(ctx, ctxKeyDigest, digest), nil
}

func ensureDigestNotExistsHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	digest, ok := ctx.Value(ctxKeyDigest).(*dockerimage.Digest)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError,
			"Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError,
			"LocalStore not set")
	}

	// Ensure file doesn't exist.
	if _, err := localStore.GetCacheFileStat(digest.Hex()); err == nil {
		return nil, NewServerError(
			http.StatusConflict,
			"Duplicate digest: %s, error: %s", digest, err)
	}
	return ctx, nil
}

func parseUUIDHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	uploadUUID := chi.URLParam(request, "uuid")
	if len(uploadUUID) == 0 {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot parse empty UUID")
	}
	if _, err := uuid.Parse(uploadUUID); err != nil {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot parse UUID: %s, error: %s", uploadUUID, err)
	}

	return context.WithValue(ctx, ctxKeyUploadUUID, uploadUUID), nil
}

func parseContentRangeHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	contentRange := request.Header.Get("Content-Range")
	if contentRange == "" {
		return nil, NewServerError(
			http.StatusBadRequest,
			"No Content-Range header")
	}

	// Parse content-range
	parts := strings.SplitN(contentRange, "-", 2)
	if len(parts) != 2 {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot parse Content-Range header: %s", contentRange)
	}
	startByte, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || startByte < 0 {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot parse start of range in Content-Range header: %s", contentRange)
	}
	ctx = context.WithValue(ctx, ctxKeyStartByte, startByte)
	endByte, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || endByte < 0 {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot parse end of range in Content-Range header: %s", contentRange)
	}

	return context.WithValue(ctx, ctxKeyEndByte, endByte), nil
}

func createUploadHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	digest, ok := ctx.Value(ctxKeyDigest).(*dockerimage.Digest)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError,
			"Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError,
			"LocalStore not set")
	}

	uploadUUID := uuid.Generate().String()
	if err := localStore.CreateUploadFile(uploadUUID, 0); err != nil {
		return nil, NewServerError(
			http.StatusInternalServerError,
			"Failed to init upload for digest: %s, error: %s", digest, err)
	}

	return context.WithValue(ctx, ctxKeyUploadUUID, uploadUUID), nil
}

func uploadBlobChunkHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError, "Digest not set")
	}
	startByte, ok := ctx.Value(ctxKeyStartByte).(int64)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError, "StartByte not set")
	}
	endByte, ok := ctx.Value(ctxKeyEndByte).(int64)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError, "EndByte not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError, "LocalStore not set")
	}

	// Get blob writer.
	// TODO: calculate SHA256 on the fly using https://github.com/stevvooe/resumable
	w, err := localStore.GetUploadFileReadWriter(uploadUUID)
	if err != nil {
		return nil, NewServerError(
			http.StatusNotFound,
			"Cannot find upload with UUID: %s, error: %s", uploadUUID, err)
	}

	blobReader, err := request.GetBody()
	if err != nil {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot get blob data for upload: %s, error: %s", uploadUUID, err)
	}
	defer blobReader.Close()

	// Seek start location.
	if _, err := w.Seek(startByte, 0); err != nil {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Cannot continue upload from offset: %d, error: %s", startByte, err)
	}
	// Write data.
	count, err := io.Copy(w, blobReader)
	if err != nil {
		return nil, NewServerError(
			http.StatusInternalServerError,
			"Failed to upload: %s, error: %s", uploadUUID, err)
	} else if endByte-startByte+1 != count {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Upload data length doesn't match content range: %s, error: %s", uploadUUID, err)
	}

	return ctx, nil
}

func commitUploadHandler(ctx context.Context, request *http.Request) (context.Context, *ServerError) {
	digest, ok := ctx.Value(ctxKeyDigest).(*dockerimage.Digest)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError, "Digest not set")
	}
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError, "uploadUUID not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerError(
			http.StatusInternalServerError, "LocalStore not set")
	}

	// Verify hash
	digester := dockerimage.NewDigester()
	reader, err := localStore.GetUploadFileReader(uploadUUID)
	if os.IsNotExist(err) {
		return nil, NewServerError(
			http.StatusNotFound,
			"Cannot find upload %s, error: %s", uploadUUID, err)
	}
	computedDigest, err := digester.FromReader(reader)
	if err != nil {
		return nil, NewServerError(
			http.StatusInternalServerError,
			"Failed to calculate digest for upload %s, error: %s", uploadUUID, err)
	} else if computedDigest.String() != digest.String() {
		return nil, NewServerError(
			http.StatusBadRequest,
			"Computed digest %s doesn't match parameter %s", computedDigest, digest)
	}

	// Commit data
	if err = localStore.MoveUploadFileToCache(uploadUUID, digest.Hex()); err != nil {
		return ctx, NewServerError(
			http.StatusInternalServerError,
			"Failed to commit digest %s for upload %s, error: %s", digest, uploadUUID, err)
	}
	return ctx, nil
}
