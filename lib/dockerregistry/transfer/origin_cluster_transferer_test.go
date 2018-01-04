package transfer

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"

	"github.com/stretchr/testify/require"
)

func TestDownloadSuccess(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOrginClusterTransfererMocks(t)
	defer cleanup()

	transferer := mocks.newTransferer()

	d, blob := image.DigestWithBlobFixture()

	clients := mocks.expectClients(d, "loc1", "loc2", "loc3")

	clients[0].EXPECT().GetBlob(d).Return(ioutil.NopCloser(bytes.NewBuffer(blob)), nil)

	result, err := transferer.Download(d.Hex())
	require.NoError(err)
	defer result.Close()
	data, err := ioutil.ReadAll(result)
	require.NoError(err)
	require.Equal(blob, data)
}

func TestDownloadFailure(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOrginClusterTransfererMocks(t)
	defer cleanup()

	transferer := mocks.newTransferer()

	d := image.DigestFixture()

	clients := mocks.expectClients(d, "loc1", "loc2", "loc3")
	for _, c := range clients {
		c.EXPECT().GetBlob(d).Return(nil, errors.New("some error"))
	}

	_, err := transferer.Download(d.Hex())
	require.Error(err)
}

func TestUploadSuccess(t *testing.T) {
	tests := []struct {
		description string
		errors      [3]error
	}{
		{
			"all succeed",
			[3]error{nil, nil, nil},
		}, {
			"blob already exists",
			[3]error{blobclient.ErrBlobExist, blobclient.ErrBlobExist, blobclient.ErrBlobExist},
		}, {
			"majority succeed",
			[3]error{nil, nil, errors.New("some error")},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newOrginClusterTransfererMocks(t)
			defer cleanup()

			transferer := mocks.newTransferer()

			d, blob := image.DigestWithBlobFixture()
			size := int64(len(blob))

			clients := mocks.expectClients(d, "loc1", "loc2", "loc3")
			for i := range test.errors {
				clients[i].EXPECT().PushBlob(d, gomock.Any(), size).Return(test.errors[i])
			}

			errc := make(chan error)
			go func() {
				errc <- transferer.Upload(d.Hex(), store.TestFileReaderCloner(blob), size)
			}()

			select {
			case err := <-errc:
				require.NoError(err)
			case <-time.After(5 * time.Second):
				panic("timeout")
			}
		})
	}
}

func TestUploadFailureMajority(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOrginClusterTransfererMocks(t)
	defer cleanup()

	transferer := mocks.newTransferer()

	d, blob := image.DigestWithBlobFixture()
	size := int64(len(blob))

	clients := mocks.expectClients(d, "loc1", "loc2", "loc3")

	// Two clients fail.
	clients[0].EXPECT().PushBlob(d, gomock.Any(), size).Return(errors.New("some error"))
	clients[1].EXPECT().PushBlob(d, gomock.Any(), size).Return(errors.New("some error"))
	clients[2].EXPECT().PushBlob(d, gomock.Any(), size).Return(nil)

	require.Error(transferer.Upload(d.Hex(), store.TestFileReaderCloner(blob), size))
}

func TestGetManifest(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOrginClusterTransfererMocks(t)
	defer cleanup()

	transferer := mocks.newTransferer()

	repo := "testrepo"
	tag := "testtag"

	rw, d, cleanup := mockManifestReadWriter()
	defer cleanup()

	mocks.manifestClient.EXPECT().GetManifest(repo, tag).Return(rw, nil)

	m, err := transferer.GetManifest(repo, tag)
	require.NoError(err)
	defer m.Close()
	ok, err := image.Verify(d, m)
	require.NoError(err)
	require.True(ok)
}

func TestPostManifest(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOrginClusterTransfererMocks(t)
	defer cleanup()

	transferer := mocks.newTransferer()

	repo := "testrepo"
	tag := "testtag"

	r, cleanup := store.NewMockFileReadWriter([]byte{})
	defer cleanup()

	mocks.manifestClient.EXPECT().PostManifest(repo, tag, r).Return(nil)
	require.NoError(transferer.PostManifest(repo, tag, r))
}
