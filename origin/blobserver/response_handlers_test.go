package blobserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"

	"code.uber.internal/infra/kraken/mocks/origin/client"
	"code.uber.internal/infra/kraken/origin/client"

	"github.com/golang/mock/gomock"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMocks struct {
	ctrl                *gomock.Controller
	blobTransferFactory client.BlobTransferFactory
}

// mockController sets up all mocks and returns a teardown func that can be called with defer
func (m *testMocks) mockController(t gomock.TestReporter) func() {
	m.ctrl = gomock.NewController(t)
	return func() {
		m.ctrl.Finish()
	}
}

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
	contentDigest, _ := image.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
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
	contentDigest, _ := image.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
	localStore.MoveUploadFileToCache(randomUUID, contentDigest.Hex())
	wrongDigest, _ := image.NewDigestFromString(image.DigestEmptyTar)
	ctx := context.WithValue(context.Background(), ctxKeyDigest, wrongDigest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mockWriter := newMockResponseWriter()
	ctx, se := downloadBlobHandler(ctx, mockWriter)
	require.Nil(ctx)
	require.NotNil(se)
	require.Equal(se.GetStatusCode(), http.StatusNotFound)
}

func helperCreateAndUploadDigest(t *testing.T, ls *store.LocalStore, us string) *image.Digest {
	ls.CreateUploadFile(us, 0)
	writer, _ := ls.GetUploadFileReadWriter(us)
	writer.Write([]byte("Hello world!!!"))

	contentDigest, _ := image.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
	ls.MoveUploadFileToCache(us, contentDigest.Hex())
	return contentDigest
}

func helperCreateTestDigests(t *testing.T, num int, shardID string,
	ls *store.LocalStore, contentDigest *image.Digest) {

	require := require.New(t)

	fullpath, err := ls.GetCacheFilePath(contentDigest.Hex())
	require.NoError(err)

	cachedir := filepath.Dir(fullpath)
	// generate context files with the same shard id
	for i := 0; i < num; i++ {
		content := []byte("Hello world!!!" + strconv.Itoa(i))
		contentDigest, _ := image.NewDigester().FromReader(strings.NewReader(string(content)))

		//re-write first shardID bytes of a content
		digest := []byte(contentDigest.Hex())
		digest[0] = shardID[0]
		digest[1] = shardID[1]
		digest[2] = shardID[2]
		digest[3] = shardID[3]

		err := ioutil.WriteFile(cachedir+"/"+string(digest), content, 0644)
		require.NoError(err)
	}
}

func verifyDigestsForRepair(t *testing.T, digests []*image.Digest, response string) {
	for _, d := range digests {
		lm := &DigestRepairMessage{
			Hostname: "host_1:1234",
			Digest:   d.Hex(),
			Result:   "OK",
		}
		dstr, _ := json.Marshal(lm)
		assert.Equal(t, true, strings.Contains(response, string(dstr)))
		lm = &DigestRepairMessage{
			Hostname: "host_2:1234",
			Digest:   d.Hex(),
			Result:   "OK",
		}
		dstr, _ = json.Marshal(lm)
		assert.Equal(t, true, strings.Contains(response, string(dstr)))
	}
}

func TestRepairBlobByShardIDHandler(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalStoreFixture()
	defer cleanup()

	app := HashConfigFixture([]int{10, 10, 10})
	hashstate := RendezvousHashFixture(app)

	contentDigest := helperCreateAndUploadDigest(t, localStore, randomUUID)

	shardID := contentDigest.Hex()[:4]
	url := fmt.Sprintf("localhost:8080/repair/shard/%s", shardID)
	request, _ := http.NewRequest("POST", url, nil)

	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("shardID", shardID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx := request.Context()
	ctx = context.WithValue(ctx, ctxKeyShardID, shardID)
	ctx = context.WithValue(ctx, ctxKeyHashConfig, app)
	ctx = context.WithValue(ctx, ctxKeyHashState, hashstate)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mocks := &testMocks{}
	defer mocks.mockController(t)()
	bt := mockclient.NewMockBlobTransferer(mocks.ctrl)

	mocks.blobTransferFactory = func(address string, blobStore *store.LocalStore) client.BlobTransferer {
		return bt
	}
	ctx = context.WithValue(ctx, ctxBlobTransferFactory, mocks.blobTransferFactory)
	request = request.WithContext(ctx)

	digests, err := localStore.ListDigests(shardID)
	require.NoError(err)

	for _, d := range digests {
		bt.EXPECT().PushBlob(*d).Return(nil)
		bt.EXPECT().PushBlob(*d).Return(nil)
	}

	w := httptest.NewRecorder()
	repairBlobByShardIDStreamHandler(request.Context(), w)
	verifyDigestsForRepair(t, digests, w.Body.String())
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRepairBlobByShardIDHandlerBatch(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalStoreFixture()
	defer cleanup()

	app := HashConfigFixture([]int{10, 10, 10})
	hashstate := RendezvousHashFixture(app)

	contentDigest := helperCreateAndUploadDigest(t, localStore, randomUUID)
	shardID := contentDigest.Hex()[:4]

	helperCreateTestDigests(t, 19, shardID, localStore, contentDigest)

	url := fmt.Sprintf("localhost:8080/repair/shard/%s", shardID)
	request, _ := http.NewRequest("POST", url, nil)

	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("shardID", shardID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx := request.Context()
	ctx = context.WithValue(ctx, ctxKeyShardID, shardID)
	ctx = context.WithValue(ctx, ctxKeyHashConfig, app)
	ctx = context.WithValue(ctx, ctxKeyHashState, hashstate)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mocks := &testMocks{}
	defer mocks.mockController(t)()
	bt := mockclient.NewMockBlobTransferer(mocks.ctrl)

	mocks.blobTransferFactory = func(address string, blobStore *store.LocalStore) client.BlobTransferer {
		return bt
	}
	ctx = context.WithValue(ctx, ctxBlobTransferFactory, mocks.blobTransferFactory)
	request = request.WithContext(ctx)

	digests, err := localStore.ListDigests(shardID)
	assert.Equal(t, 20, len(digests))
	require.NoError(err)

	for _, d := range digests {
		// first node
		bt.EXPECT().PushBlob(*d).Return(nil)
		// second node
		bt.EXPECT().PushBlob(*d).Return(nil)
	}

	w := httptest.NewRecorder()
	repairBlobByShardIDStreamHandler(request.Context(), w)

	verifyDigestsForRepair(t, digests, w.Body.String())
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRepairBatchBlobByShardIDHandlerRetry(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalStoreFixture()
	defer cleanup()

	app := HashConfigFixture([]int{10, 10, 10})
	hashstate := RendezvousHashFixture(app)

	contentDigest := helperCreateAndUploadDigest(t, localStore, randomUUID)
	shardID := contentDigest.Hex()[:4]

	url := fmt.Sprintf("localhost:8080/repair/shard/%s", shardID)
	request, _ := http.NewRequest("POST", url, nil)

	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("shardID", shardID)
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeCtx))

	ctx := request.Context()
	ctx = context.WithValue(ctx, ctxKeyShardID, shardID)
	ctx = context.WithValue(ctx, ctxKeyHashConfig, app)
	ctx = context.WithValue(ctx, ctxKeyHashState, hashstate)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mocks := &testMocks{}
	defer mocks.mockController(t)()
	bt := mockclient.NewMockBlobTransferer(mocks.ctrl)

	mocks.blobTransferFactory = func(address string, blobStore *store.LocalStore) client.BlobTransferer {
		return bt
	}
	ctx = context.WithValue(ctx, ctxBlobTransferFactory, mocks.blobTransferFactory)

	request = request.WithContext(ctx)

	digests, err := localStore.ListDigests(shardID)
	require.NoError(err)

	for _, d := range digests {
		bt.EXPECT().PushBlob(*d).Return(fmt.Errorf("oh god"))
		bt.EXPECT().PushBlob(*d).Return(nil)
		bt.EXPECT().PushBlob(*d).Return(nil)
	}

	w := httptest.NewRecorder()
	repairBlobByShardIDStreamHandler(request.Context(), w)

	verifyDigestsForRepair(t, digests, w.Body.String())
	assert.Equal(t, http.StatusOK, w.Code)
}
