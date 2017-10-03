package blobserver

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/docker/distribution/uuid"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

func TestCheckBlobHandlerOK(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, nil)

	_, err := httputil.Head(fmt.Sprintf("http://%s/blobs/%s", addr, d))
	require.NoError(err)
}

func TestCheckBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, os.ErrNotExist)

	_, err := httputil.Head(fmt.Sprintf("http://%s/blobs/%s", addr, d))
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestGetBlobHandlerOK(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	blob := randutil.Text(256)
	f, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	mocks.fileStore.EXPECT().GetCacheFileReader(d.Hex()).Return(f, nil)

	r, err := httputil.Get(fmt.Sprintf("http://%s/blobs/%s", addr, d))
	require.NoError(err)
	testutil.ExpectBody(t, r, blob)
}

func TestGetBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileReader(d.Hex()).Return(nil, os.ErrNotExist)

	_, err := httputil.Get(fmt.Sprintf("http://%s/blobs/%s", addr, d))
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestDeleteBlobHandlerAccepted(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().MoveCacheFileToTrash(d.Hex()).Return(nil)

	_, err := httputil.Delete(
		fmt.Sprintf("http://%s/blobs/%s", addr, d),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	require.NoError(err)
}

func TestDeleteBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().MoveCacheFileToTrash(d.Hex()).Return(os.ErrNotExist)

	_, err := httputil.Delete(fmt.Sprintf("http://%s/blobs/%s", addr, d))
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestUploadBlobHandlerAccepted(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, os.ErrNotExist)
	mocks.fileStore.EXPECT().CreateUploadFile(gomock.Any(), int64(0)).Return(nil)

	_, err := httputil.Post(
		fmt.Sprintf("http://%s/blobs/%s/uploads", addr, d),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	require.NoError(err)
}

func TestUploadBlobHandlerConflict(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, nil)

	_, err := httputil.Post(fmt.Sprintf("http://%s/blobs/%s/uploads", addr, d))
	require.Error(err)
	require.Equal(http.StatusConflict, err.(httputil.StatusError).Status)
}

func TestPatchUploadHandlerAccepted(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, serverStop := testServer(configNoRedirectFixture(), mocks)
	defer serverStop()

	u := uuid.Generate().String()
	d := image.DigestFixture()

	blob := randutil.Text(256)
	f, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	chunk := randutil.Text(64)
	start := 128
	stop := start + len(chunk)

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, os.ErrNotExist)
	mocks.fileStore.EXPECT().GetUploadFileReadWriter(u).Return(f, nil)

	r, err := httputil.Patch(
		fmt.Sprintf("http://%s/blobs/%s/uploads/%s", addr, d, u),
		httputil.SendBody(bytes.NewBuffer(chunk)),
		httputil.SendHeaders(map[string]string{
			"Content-Range": fmt.Sprintf("%d-%d", start, stop),
		}),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	require.NoError(err)
	require.Equal(r.Header.Get("Location"), "/blobs/uploads/"+u)

	content, err := ioutil.ReadFile(f.Name())
	require.NoError(err)
	require.Equal(
		string(blob[:start])+string(chunk)+string(blob[stop:]), string(content))
}

func TestPatchUploadHandlerConflict(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, serverStop := testServer(configNoRedirectFixture(), mocks)
	defer serverStop()

	u := uuid.Generate().String()
	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, nil)

	_, err := httputil.Patch(fmt.Sprintf("http://%s/blobs/%s/uploads/%s", addr, d, u))
	require.Error(err)
	require.Equal(http.StatusConflict, err.(httputil.StatusError).Status)
}

func TestCommitUploadHandlerCreated(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	u := uuid.Generate().String()

	blob := randutil.Text(256)
	f, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	d, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	mocks.fileStore.EXPECT().GetUploadFileReader(u).Return(f, nil)
	mocks.fileStore.EXPECT().MoveUploadFileToCache(u, d.Hex()).Return(nil)

	_, err = httputil.Put(
		fmt.Sprintf("http://%s/blobs/%s/uploads/%s", addr, d, u),
		httputil.SendAcceptedCodes(http.StatusCreated))
	require.NoError(err)
}

func TestCommitUploadHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := testServer(configNoRedirectFixture(), mocks)
	defer stop()

	u := uuid.Generate().String()
	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetUploadFileReader(u).Return(nil, os.ErrNotExist)

	_, err := httputil.Put(fmt.Sprintf("http://%s/blobs/%s/uploads/%s", addr, d, u))
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestParseContentRangeHeaderBadRequests(t *testing.T) {
	tests := []struct {
		description string
		value       string
	}{
		{"empty value", ""},
		{"invalid format", "blah"},
		{"invalid start", "blah-5"},
		{"invalid end", "5-blah"},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			h := http.Header{}
			h.Add("Content-Range", test.value)
			start, end, err := parseContentRange(h)
			require.Error(err)
			require.Equal(http.StatusBadRequest, err.(*serverError).status)
			require.Equal(int64(0), start)
			require.Equal(int64(0), end)
		})
	}
}

func TestHandlersRedirectByDigest(t *testing.T) {
	d := image.DigestFixture()
	u := uuid.Generate().String()

	tests := []struct {
		method   string
		endpoint string
	}{
		{"HEAD", fmt.Sprintf("/blobs/%s", d)},
		{"GET", fmt.Sprintf("/blobs/%s", d)},
		{"POST", fmt.Sprintf("/blobs/%s/uploads", d)},
		{"PATCH", fmt.Sprintf("/blobs/%s/uploads/%s", d, u)},
		{"PUT", fmt.Sprintf("/blobs/%s/uploads/%s", d, u)},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.endpoint, func(t *testing.T) {
			require := require.New(t)

			mocks := newServerMocks(t)
			defer mocks.ctrl.Finish()

			// Set the master we test against to have a weight of 0, such that all
			// requests will redirect to the other nodes.
			config := configFixture()
			node := config.HashNodes[testMaster]
			node.Weight = 0
			config.HashNodes[testMaster] = node

			addr, stop := testServer(config, mocks)
			defer stop()

			r, err := httputil.Send(
				test.method,
				fmt.Sprintf("http://%s%s", addr, test.endpoint),
				httputil.SendAcceptedCodes(http.StatusTemporaryRedirect))
			require.NoError(err)
			require.Equal("origin2,origin3", r.Header.Get("Origin-Locations"))
		})
	}
}
