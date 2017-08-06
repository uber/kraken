package service

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"code.uber.internal/infra/kraken/client/dockerimage"

	"github.com/pressly/chi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDigestHandlerValid(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/%s", dockerimage.DigestEmptyTar)
	request, _ := http.NewRequest("GET", url, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("digest", dockerimage.DigestEmptyTar)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseDigestHandler(request.Context(), request)
	require.Nil(se)
	digest, ok := ctx.Value(ctxKeyDigest).(*dockerimage.Digest)
	require.True(ok)
	require.Equal(digest.String(), dockerimage.DigestEmptyTar)
}

func TestParseDigestHandlerNoAlgo(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/%s", emptyDigestHex)
	request, _ := http.NewRequest("GET", url, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("digest", emptyDigestHex)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseDigestHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestParseDigestHandlerEmpty(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/")
	request, _ := http.NewRequest("GET", url, nil)
	routeCtx := chi.NewRouteContext()
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseDigestHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestParseDigestFromQueryHandlerValid(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads?digest=%s", dockerimage.DigestEmptyTar)
	request, _ := http.NewRequest("POST", url, nil)

	ctx, se := parseDigestFromQueryHandler(request.Context(), request)
	require.Nil(se)
	digest, ok := ctx.Value(ctxKeyDigest).(*dockerimage.Digest)
	require.True(ok)
	require.Equal(digest.String(), dockerimage.DigestEmptyTar)
}

func TestParseDigestFromQueryHandlerNoAlgo(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads?digest=%s", emptyDigestHex)
	request, _ := http.NewRequest("POST", url, nil)

	ctx, se := parseDigestFromQueryHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestParseDigestFromQueryHandlerEmpty(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads")
	request, _ := http.NewRequest("POST", url, nil)

	ctx, se := parseDigestFromQueryHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestEnsureDigestNotExistsHandlerValid(t *testing.T) {
	require := require.New(t)

	c, localStore := getLocalStore()
	defer clearLocalStore(c)

	url := fmt.Sprintf("localhost:8080/blob/uploads?digest=%s", dockerimage.DigestEmptyTar)
	request, _ := http.NewRequest("POST", url, nil)
	ctx := request.Context()
	digest, _ := dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx = context.WithValue(ctx, ctxKeyDigest, digest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	_, se := ensureDigestNotExistsHandler(request.Context(), request)
	require.Nil(se)
}

func TestEnsureDigestNotExistsHandlerConflict(t *testing.T) {
	require := require.New(t)

	c, localStore := getLocalStore()
	localStore.CreateDownloadFile(emptyDigestHex, 0)
	localStore.MoveDownloadFileToCache(emptyDigestHex)
	defer clearLocalStore(c)

	url := fmt.Sprintf("localhost:8080/blob/uploads?digest=%s", dockerimage.DigestEmptyTar)
	request, _ := http.NewRequest("POST", url, nil)
	ctx := request.Context()
	digest, _ := dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx = context.WithValue(ctx, ctxKeyDigest, digest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	ctx, se := ensureDigestNotExistsHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusConflict)
}

func TestParseUUIDHandlerValid(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads/%s", randomUUID)
	request, _ := http.NewRequest("PATCH", url, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("uuid", randomUUID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseUUIDHandler(request.Context(), request)
	require.Nil(se)
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	require.True(ok)
	require.Equal(uploadUUID, randomUUID)
}

func TestParseUUIDHandlerInvalid(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads/%s", "b9cb2c15")
	request, _ := http.NewRequest("PATCH", url, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("uuid", "b9cb2c15")
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseUUIDHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestParseUUIDHandlerEmpty(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads/")
	request, _ := http.NewRequest("PATCH", url, nil)
	routeCtx := chi.NewRouteContext()
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseUUIDHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestParseContentRangeHandlerValid(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads/%s", randomUUID)
	request, _ := http.NewRequest("PATCH", url, nil)
	request.Header.Set("content-range", "5-10")
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("uuid", randomUUID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseContentRangeHandler(request.Context(), request)
	require.Nil(se)
	startByte, ok := ctx.Value(ctxKeyStartByte).(int64)
	require.True(ok)
	require.Equal(startByte, int64(5))
	endByte, ok := ctx.Value(ctxKeyEndByte).(int64)
	require.True(ok)
	require.Equal(endByte, int64(10))
}

func TestParseContentRangeHandlerInvalid(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads/%s", randomUUID)
	request, _ := http.NewRequest("PATCH", url, nil)
	request.Header.Set("content-range", "5--10")
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("uuid", randomUUID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseContentRangeHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)

	request, _ = http.NewRequest("PATCH", url, nil)
	request.Header.Set("content-range", " 5-10")
	routeCtx = chi.NewRouteContext()
	routeCtx.URLParams.Add("uuid", randomUUID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se = parseContentRangeHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
	require.True(strings.HasPrefix(se.Error(), "Cannot parse start of range"))

	request, _ = http.NewRequest("PATCH", url, nil)
	request.Header.Set("content-range", "-1-10")
	routeCtx = chi.NewRouteContext()
	routeCtx.URLParams.Add("uuid", randomUUID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se = parseContentRangeHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestParseContentRangeHandlerEmpty(t *testing.T) {
	require := require.New(t)

	url := fmt.Sprintf("localhost:8080/blob/uploads/%s", randomUUID)
	request, _ := http.NewRequest("PATCH", url, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("uuid", randomUUID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx, se := parseContentRangeHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
}

func TestCreateUploadHandler(t *testing.T) {
	require := require.New(t)

	c, localStore := getLocalStore()
	defer clearLocalStore(c)

	url := fmt.Sprintf("localhost:8080/blob/uploads?digest=%s", dockerimage.DigestEmptyTar)
	request, _ := http.NewRequest("POST", url, nil)
	ctx := request.Context()
	digest, _ := dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx = context.WithValue(ctx, ctxKeyDigest, digest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	ctx, se := createUploadHandler(request.Context(), request)
	require.Nil(se)
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	assert.True(t, ok)
	_, err := localStore.GetUploadFileReader(uploadUUID)
	require.NoError(err)
}

func TestUploadBlobChunkHandler(t *testing.T) {
	require := require.New(t)

	c, localStore := getLocalStore()
	defer clearLocalStore(c)
	localStore.CreateUploadFile(randomUUID, 0)

	url := fmt.Sprintf("localhost:8080/blob/uploads?digest=%s", dockerimage.DigestEmptyTar)
	request, _ := http.NewRequest("PATCH", url, strings.NewReader("Hello!"))
	request.Header.Set("content-range", "0-5")
	ctx := request.Context()
	digest, _ := dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx = context.WithValue(ctx, ctxKeyDigest, digest)
	ctx = context.WithValue(ctx, ctxKeyUploadUUID, randomUUID)
	ctx = context.WithValue(ctx, ctxKeyStartByte, int64(0))
	ctx = context.WithValue(ctx, ctxKeyEndByte, int64(5))
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	ctx, se := uploadBlobChunkHandler(request.Context(), request)
	require.Nil(se)
	reader, err := localStore.GetUploadFileReader(randomUUID)
	require.NoError(err)
	savedContent, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(savedContent, []byte("Hello!"))

	// Overlap
	url = fmt.Sprintf("localhost:8080/blob/uploads?digest=%s", dockerimage.DigestEmptyTar)
	request, _ = http.NewRequest("PATCH", url, strings.NewReader(" world!"))
	request.Header.Set("content-range", "5-11")
	ctx = request.Context()
	digest, _ = dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx = context.WithValue(ctx, ctxKeyDigest, digest)
	ctx = context.WithValue(ctx, ctxKeyUploadUUID, randomUUID)
	ctx = context.WithValue(ctx, ctxKeyStartByte, int64(5))
	ctx = context.WithValue(ctx, ctxKeyEndByte, int64(11))
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	ctx, se = uploadBlobChunkHandler(request.Context(), request)
	require.Nil(se)
	reader, err = localStore.GetUploadFileReader(randomUUID)
	require.NoError(err)
	savedContent, err = ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(savedContent, []byte("Hello world!"))

	// Gap
	url = fmt.Sprintf("localhost:8080/blob/uploads/%s?digest=%s", randomUUID, dockerimage.DigestEmptyTar)
	request, _ = http.NewRequest("PATCH", url, strings.NewReader("kraken"))
	request.Header.Set("content-range", "100-105")
	ctx = request.Context()
	digest, _ = dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx = context.WithValue(ctx, ctxKeyDigest, digest)
	ctx = context.WithValue(ctx, ctxKeyUploadUUID, randomUUID)
	ctx = context.WithValue(ctx, ctxKeyStartByte, int64(100))
	ctx = context.WithValue(ctx, ctxKeyEndByte, int64(105))
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	ctx, se = uploadBlobChunkHandler(request.Context(), request)
	require.Nil(se)
	reader, err = localStore.GetUploadFileReader(randomUUID)
	require.NoError(err)
	buf := make([]byte, 100)
	count, err := reader.ReadAt(buf, int64(100))
	require.Equal(err, io.EOF)
	require.Equal(count, 6)
	require.Equal(buf[:6], []byte("kraken"))
}

func TestCommitUploadHandlerValid(t *testing.T) {
	require := require.New(t)

	c, localStore := getLocalStore()
	defer clearLocalStore(c)
	localStore.CreateUploadFile(randomUUID, 0)
	writer, _ := localStore.GetUploadFileReadWriter(randomUUID)
	writer.Write([]byte("Hello world!!!"))
	contentDigest, se := dockerimage.NewDigester().FromReader(strings.NewReader("Hello world!!!"))

	url := fmt.Sprintf("localhost:8080/blob/uploads/%s?digest=%s", randomUUID, contentDigest.String())
	request, _ := http.NewRequest("PUT", url, nil)
	request.Header.Set("content-range", "0-5")
	ctx := request.Context()
	ctx = context.WithValue(ctx, ctxKeyDigest, contentDigest)
	ctx = context.WithValue(ctx, ctxKeyUploadUUID, randomUUID)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	ctx, se = commitUploadHandler(request.Context(), request)
	require.Nil(se)
	reader, err := localStore.GetCacheFileReader(contentDigest.Hex())
	require.NoError(err)
	savedContent, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(savedContent, []byte("Hello world!!!"))
}

func TestCommitUploadHandlerInvalidDigest(t *testing.T) {
	require := require.New(t)

	c, localStore := getLocalStore()
	defer clearLocalStore(c)
	localStore.CreateUploadFile(randomUUID, 0)
	writer, _ := localStore.GetUploadFileReadWriter(randomUUID)
	writer.Write([]byte("Hello world!!!"))

	url := fmt.Sprintf("localhost:8080/blob/uploads/%s?digest=%s", randomUUID, dockerimage.DigestEmptyTar)
	request, _ := http.NewRequest("PUT", url, nil)
	request.Header.Set("content-range", "0-5")
	ctx := request.Context()
	wrongDigest, _ := dockerimage.NewDigestFromString(dockerimage.DigestEmptyTar)
	ctx = context.WithValue(ctx, ctxKeyDigest, wrongDigest)
	ctx = context.WithValue(ctx, ctxKeyUploadUUID, randomUUID)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	request = request.WithContext(ctx)

	ctx, se := commitUploadHandler(request.Context(), request)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.StatusCode(), http.StatusBadRequest)
	assert.True(t, strings.HasPrefix(se.Error(), "Computed digest"))
}
