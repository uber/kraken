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

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"

	"code.uber.internal/infra/kraken/mocks/origin/client"
	"code.uber.internal/infra/kraken/origin/client"

	"github.com/golang/mock/gomock"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadBlobHandlerValid(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalFileStoreFixture()
	defer cleanup()
	localStore.CreateUploadFile(randomUUID, 0)
	writer, _ := localStore.GetUploadFileReadWriter(randomUUID)
	writer.Write([]byte("Hello world!!!"))
	contentDigest, _ := image.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
	localStore.MoveUploadFileToCache(randomUUID, contentDigest.Hex())
	ctx := context.WithValue(context.Background(), ctxKeyDigest, contentDigest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mockWriter := httptest.NewRecorder()

	ctx, se := downloadBlobHandler(ctx, mockWriter)
	require.Nil(se)
	require.Equal(mockWriter.Body.String(), "Hello world!!!")
}

func TestDownloadBlobHandlerInvalid(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalFileStoreFixture()
	defer cleanup()
	localStore.CreateUploadFile(randomUUID, 0)
	writer, _ := localStore.GetUploadFileReadWriter(randomUUID)
	writer.Write([]byte("Hello world!!!"))
	contentDigest, _ := image.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
	localStore.MoveUploadFileToCache(randomUUID, contentDigest.Hex())
	wrongDigest, _ := image.NewDigestFromString(image.DigestEmptyTar)
	ctx := context.WithValue(context.Background(), ctxKeyDigest, wrongDigest)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mockWriter := httptest.NewRecorder()
	ctx, se := downloadBlobHandler(ctx, mockWriter)
	require.Nil(ctx)
	require.Error(se)
	require.Equal(se.GetStatusCode(), http.StatusNotFound)
}

func helperCreateAndUploadDigest(t *testing.T, ls store.FileStore, us string) *image.Digest {
	ls.CreateUploadFile(us, 0)
	writer, _ := ls.GetUploadFileReadWriter(us)
	writer.Write([]byte("Hello world!!!"))

	contentDigest, _ := image.NewDigester().FromReader(strings.NewReader("Hello world!!!"))
	ls.MoveUploadFileToCache(us, contentDigest.Hex())
	return contentDigest
}

func helperCreateTestDigests(t *testing.T, num int, shardID string,
	ls store.FileStore, contentDigest *image.Digest) {

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

func TestRepairBlobByShardIDSingleDigestReturnsOK(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	app := configFixture([]int{10, 10, 10})
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

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	bt := mockclient.NewMockBlobTransferer(mockCtrl)
	bf := client.BlobTransferFactory(
		func(address string, blobStore store.FileStore) client.BlobTransferer {
			return bt
		})

	ctx = context.WithValue(ctx, ctxBlobTransferFactory, bf)
	request = request.WithContext(ctx)
	names, err := localStore.ListCacheFilesByShardID(shardID)
	require.NoError(err)
	var digests []*image.Digest
	for _, name := range names {
		digest, err := image.NewDigestFromString("sha256:" + name)
		require.NoError(err)
		digests = append(digests, digest)
	}

	for _, d := range digests {
		bt.EXPECT().PushBlob(*d).Return(nil)
		bt.EXPECT().PushBlob(*d).Return(nil)
	}

	w := httptest.NewRecorder()
	repairBlobStreamHandler(request.Context(), w)
	verifyDigestsForRepair(t, digests, w.Body.String())
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRepairBlobByShardIDHandlerBatchReturnsOK(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	app := configFixture([]int{10, 10, 10})
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

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	bt := mockclient.NewMockBlobTransferer(mockCtrl)
	bf := client.BlobTransferFactory(
		func(address string, blobStore store.FileStore) client.BlobTransferer {
			return bt
		})

	ctx = context.WithValue(ctx, ctxBlobTransferFactory, bf)
	request = request.WithContext(ctx)

	names, err := localStore.ListCacheFilesByShardID(shardID)
	require.NoError(err)
	var digests []*image.Digest
	for _, name := range names {
		digest, err := image.NewDigestFromString("sha256:" + name)
		require.NoError(err)
		digests = append(digests, digest)
	}
	require.Equal(20, len(digests))
	require.NoError(err)

	for _, d := range digests {
		// first node
		bt.EXPECT().PushBlob(*d).Return(nil)
		// second node
		bt.EXPECT().PushBlob(*d).Return(nil)
	}

	w := httptest.NewRecorder()
	repairBlobStreamHandler(request.Context(), w)

	verifyDigestsForRepair(t, digests, w.Body.String())
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRepairBatchBlobByShardIDHandlerFailAndRetryOK(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	app := configFixture([]int{10, 10, 10})
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

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	bt := mockclient.NewMockBlobTransferer(mockCtrl)
	bf := client.BlobTransferFactory(
		func(address string, blobStore store.FileStore) client.BlobTransferer {
			return bt
		})

	ctx = context.WithValue(ctx, ctxBlobTransferFactory, bf)
	request = request.WithContext(ctx)
	names, err := localStore.ListCacheFilesByShardID(shardID)
	require.NoError(err)
	var digests []*image.Digest
	for _, name := range names {
		digest, err := image.NewDigestFromString("sha256:" + name)
		require.NoError(err)
		digests = append(digests, digest)
	}

	for _, d := range digests {
		bt.EXPECT().PushBlob(*d).Return(fmt.Errorf("oh god"))
		bt.EXPECT().PushBlob(*d).Return(nil)
		bt.EXPECT().PushBlob(*d).Return(nil)
	}

	w := httptest.NewRecorder()
	repairBlobStreamHandler(request.Context(), w)

	verifyDigestsForRepair(t, digests, w.Body.String())
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRepairBlobByDigestReturnsOK(t *testing.T) {
	localStore, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	app := configFixture([]int{10, 10, 10})
	hashstate := RendezvousHashFixture(app)

	contentDigest := helperCreateAndUploadDigest(t, localStore, randomUUID)

	url := fmt.Sprintf("localhost:8080/repair/%s", contentDigest)
	request, _ := http.NewRequest("POST", url, nil)

	ctx := request.Context()
	ctx = context.WithValue(ctx, ctxKeyDigest, contentDigest)
	ctx = context.WithValue(ctx, ctxKeyHashConfig, app)
	ctx = context.WithValue(ctx, ctxKeyHashState, hashstate)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	bt := mockclient.NewMockBlobTransferer(mockCtrl)
	bf := client.BlobTransferFactory(
		func(address string, blobStore store.FileStore) client.BlobTransferer {
			return bt
		})

	ctx = context.WithValue(ctx, ctxBlobTransferFactory, bf)
	request = request.WithContext(ctx)

	digests := []*image.Digest{contentDigest}

	// first node
	bt.EXPECT().PushBlob(*digests[0]).Return(nil)

	// second node
	bt.EXPECT().PushBlob(*digests[0]).Return(nil)

	w := httptest.NewRecorder()
	repairBlobStreamHandler(request.Context(), w)

	verifyDigestsForRepair(t, digests, w.Body.String())
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRepairBlobByDigestCancelRequest(t *testing.T) {
	require := require.New(t)

	localStore, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	app := configFixture([]int{10, 10, 10})
	hashstate := RendezvousHashFixture(app)

	contentDigest := helperCreateAndUploadDigest(t, localStore, randomUUID)

	url := fmt.Sprintf("localhost:8080/repair/%s", contentDigest)
	request, _ := http.NewRequest("POST", url, nil)

	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("digest", contentDigest.Hex())

	ctx, cancel := context.WithCancel(context.Background())

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx = context.WithValue(ctx, chi.RouteCtxKey, routeCtx)
	ctx = context.WithValue(ctx, ctxKeyDigest, contentDigest)
	ctx = context.WithValue(ctx, ctxKeyHashConfig, app)
	ctx = context.WithValue(ctx, ctxKeyHashState, hashstate)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)

	bt := mockclient.NewMockBlobTransferer(mockCtrl)
	bf := client.BlobTransferFactory(
		func(address string, blobStore store.FileStore) client.BlobTransferer {
			return bt
		})

	ctx = context.WithValue(ctx, ctxBlobTransferFactory, bf)
	request = request.WithContext(ctx)

	// first node
	shardID := contentDigest.Hex()[:4]
	names, err := localStore.ListCacheFilesByShardID(shardID)
	require.NoError(err)
	var digests []*image.Digest
	for _, name := range names {
		digest, err := image.NewDigestFromString("sha256:" + name)
		require.NoError(err)
		digests = append(digests, digest)
	}
	require.NoError(err)

	//This will repair a single item on a first node,
	//second node repair should be cancelled due to
	//request context being cancelled.
	for _, d := range digests {
		bt.EXPECT().PushBlob(*d).Do(func(interface{}) { cancel() }).Return(nil)
	}
	w := httptest.NewRecorder()
	repairBlobStreamHandler(request.Context(), w)

	lm := &DigestRepairMessage{}
	response, err := ioutil.ReadAll(w.Body)

	_ = json.Unmarshal(response, &lm)

	//This should be no-op to fool golang escape analisys
	//that reports cancel as a not being used variable - wrong!.
	cancel()

	assert.Equal(t, lm.Digest, contentDigest.Hex())
	assert.Equal(t, http.StatusOK, w.Code)
}
