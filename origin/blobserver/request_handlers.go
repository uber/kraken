package blobserver

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/hrw"
	hashcfg "code.uber.internal/infra/kraken/origin/config"

	"github.com/docker/distribution/uuid"
	"github.com/pressly/chi"
)

func parseDigestHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digestParam := chi.URLParam(request, "digest")
	if len(digestParam) == 0 {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Failed to parse an empty digest")
	}

	var err error
	digestRaw, err := url.QueryUnescape(digestParam)
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot unescape digest: %s, error: %s", digestParam, err)
	}
	digest, err := image.NewDigestFromString(digestRaw)
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot parse digest: %s, error: %s", digestRaw, err)
	}
	return context.WithValue(ctx, ctxKeyDigest, digest), nil
}

func parseDigestFromQueryHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digestParam := request.URL.Query().Get("digest")
	if len(digestParam) == 0 {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Failed to parse an empty digest")
	}

	var err error
	digestRaw, err := url.QueryUnescape(digestParam)
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot unescape digest: %s, error: %s", digestParam, err)
	}
	digest, err := image.NewDigestFromString(digestRaw)
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot parse digest: %s, error: %s", digestRaw, err)
	}
	return context.WithValue(ctx, ctxKeyDigest, digest), nil
}

func ensureDigestExistsHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"LocalStore not set")
	}

	// Ensure file exists.
	if _, err := localStore.GetCacheFileStat(digest.Hex()); err != nil {
		if os.IsNotExist(err) {
			return nil, NewServerResponseWithError(
				http.StatusNotFound,
				"Cannot find blob data with digest: %s, error: %s", digest, err)
		}
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Error happened when looking for blob data with digest: %s, error: %s", digest, err)
	}
	return ctx, nil
}

func ensureDigestNotExistsHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"LocalStore not set")
	}

	// Ensure file doesn't exist.
	if _, err := localStore.GetCacheFileStat(digest.Hex()); err == nil {
		return nil, NewServerResponseWithError(
			http.StatusConflict,
			"Duplicate digest: %s, error: %s", digest, err)
	}
	return ctx, nil
}

func parseUUIDHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	uploadUUID := chi.URLParam(request, "uuid")
	if len(uploadUUID) == 0 {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot parse empty UUID")
	}
	if _, err := uuid.Parse(uploadUUID); err != nil {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot parse UUID: %s, error: %s", uploadUUID, err)
	}

	return context.WithValue(ctx, ctxKeyUploadUUID, uploadUUID), nil
}

func parseContentRangeHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	contentRange := request.Header.Get("Content-Range")
	if contentRange == "" {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"No Content-Range header")
	}

	// Parse content-range
	parts := strings.SplitN(contentRange, "-", 2)
	if len(parts) != 2 {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot parse Content-Range header: %s", contentRange)
	}
	startByte, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || startByte < 0 {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot parse start of range in Content-Range header: %s", contentRange)
	}
	ctx = context.WithValue(ctx, ctxKeyStartByte, startByte)
	endByte, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || endByte < 0 {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot parse end of range in Content-Range header: %s", contentRange)
	}

	return context.WithValue(ctx, ctxKeyEndByte, endByte), nil
}

func createUploadHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"LocalStore not set")
	}

	uploadUUID := uuid.Generate().String()
	if err := localStore.CreateUploadFile(uploadUUID, 0); err != nil {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Failed to init upload for digest: %s, error: %s", digest, err)
	}

	return context.WithValue(ctx, ctxKeyUploadUUID, uploadUUID), nil
}

func uploadBlobChunkHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "Digest not set")
	}
	startByte, ok := ctx.Value(ctxKeyStartByte).(int64)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "StartByte not set")
	}
	endByte, ok := ctx.Value(ctxKeyEndByte).(int64)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "EndByte not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "LocalStore not set")
	}

	// Get blob writer.
	// TODO: calculate SHA256 on the fly using https://github.com/stevvooe/resumable
	w, err := localStore.GetUploadFileReadWriter(uploadUUID)
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusNotFound,
			"Cannot find upload with UUID: %s, error: %s", uploadUUID, err)
	}

	blobReader, err := request.GetBody()
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot get blob data for upload: %s, error: %s", uploadUUID, err)
	}
	defer blobReader.Close()

	// Seek start location.
	if _, err := w.Seek(startByte, 0); err != nil {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Cannot continue upload from offset: %d, error: %s", startByte, err)
	}
	// Write data.
	count, err := io.Copy(w, blobReader)
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Failed to upload: %s, error: %s", uploadUUID, err)
	} else if endByte-startByte+1 != count {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Upload data length doesn't match content range: %s, error: %s", uploadUUID, err)
	}

	return ctx, nil
}

func commitUploadHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "Digest not set")
	}
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "uploadUUID not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "LocalStore not set")
	}

	// Verify hash.
	digester := image.NewDigester()
	reader, err := localStore.GetUploadFileReader(uploadUUID)
	if os.IsNotExist(err) {
		return nil, NewServerResponseWithError(
			http.StatusNotFound,
			"Cannot find upload %s, error: %s", uploadUUID, err)
	}
	computedDigest, err := digester.FromReader(reader)
	if err != nil {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Failed to calculate digest for upload %s, error: %s", uploadUUID, err)
	} else if computedDigest.String() != digest.String() {
		return nil, NewServerResponseWithError(
			http.StatusBadRequest,
			"Computed digest %s doesn't match parameter %s", computedDigest, digest)
	}

	// Commit data
	if err = localStore.MoveUploadFileToCache(uploadUUID, digest.Hex()); err != nil {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Failed to commit digest %s for upload %s, error: %s", digest, uploadUUID, err)
	}
	return ctx, nil
}

func deleteBlobHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "LocalStore not set")
	}

	if err := localStore.MoveCacheFileToTrash(digest.Hex()); err != nil {
		if os.IsNotExist(err) {
			return nil, NewServerResponseWithError(http.StatusNotFound, "Cannot find blob data with digest: %s, error: %s", digest, err)
		}
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "Cannot delete blob data with digest: %s, error: %s", digest, err)
	}

	return ctx, nil
}

// Find out which node(s) are responsible for the blob based on the first 2 bytes of digest.
// Do nothing if current node is among the designated nodes.
// TODO: this node could be new. This method should ensure the file is actually available locally.
func redirectByDigestHandler(ctx context.Context, request *http.Request) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "Digest not set")
	}
	hashConfig, ok := ctx.Value(ctxKeyHashConfig).(hashcfg.HashConfig)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "label not set")
	}
	hashState, ok := ctx.Value(ctxKeyHashState).(*hrw.RendezvousHash)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "HashState not set")
	}

	// Shard by first 2 bytes of digest.
	shardID := digest.Hex()[:4]
	nodes, err := hashState.GetOrderedNodes(shardID, hashConfig.NumReplica)
	if err != nil || len(nodes) == 0 {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"Failed to calculate hash for digest %s, error: %s", digest, err)
	}
	var labels []string
	for _, node := range nodes {
		if node.Label == hashConfig.Label {
			// Current node is among the designated nodes, return.
			return ctx, nil
		}
		labels = append(labels, node.Label)
	}

	// Return origin hosts that should have the blob.
	labelsStr := strings.Join(labels, ",")
	resp := NewServerResponse(http.StatusTemporaryRedirect)
	resp.AddHeader("Origin-Locations", labelsStr)

	return ctx, resp
}
