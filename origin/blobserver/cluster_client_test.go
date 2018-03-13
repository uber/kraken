package blobserver

import (
	"bytes"
	"io/ioutil"
	"sort"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func toAddrs(clients []blobclient.Client) []string {
	var addrs []string
	for _, c := range clients {
		addrs = append(addrs, c.Addr())
	}
	sort.Strings(addrs)
	return addrs
}

func TestClusterClientResilientToUnavailableMasters(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	// Register a dummy master addresses so Provide can still create a Client for
	// unavailable masters.
	cp.register(master2, "http://localhost:0")
	cp.register(master3, "http://localhost:0")

	cc := blobclient.NewClusterClient(
		blobclient.NewClientResolver(cp, serverset.MustRoundRobin(master1, master2, master3)))

	// Run many times to make sure we eventually hit unavailable masters.
	for i := 0; i < 100; i++ {
		blob := core.NewBlobFixture()

		require.NoError(cc.UploadBlob("noexist", blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content))))

		mi, err := cc.GetMetaInfo("noexist", blob.Digest)
		require.NoError(err)
		require.NotNil(mi)

		r, err := cc.DownloadBlob(blob.Digest)
		require.NoError(err)
		result, err := ioutil.ReadAll(r)
		require.NoError(err)
		require.Equal(string(blob.Content), string(result))

		peers, err := cc.Owners(blob.Digest)
		require.NoError(err)
		require.Len(peers, 1)
		require.Equal(s.pctx, peers[0])
	}
}

func TestClusterClientReturnsErrorOnNoAvailability(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()
	cp.register(master1, "http://localhost:0")
	cp.register(master2, "http://localhost:0")
	cp.register(master3, "http://localhost:0")

	cc := blobclient.NewClusterClient(
		blobclient.NewClientResolver(cp, serverset.MustRoundRobin(master1, master2, master3)))

	blob := core.NewBlobFixture()

	require.Error(cc.UploadBlob("noexist", blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content))))

	_, err := cc.GetMetaInfo("noexist", blob.Digest)
	require.Error(err)

	_, err = cc.DownloadBlob(blob.Digest)
	require.Error(err)

	_, err = cc.Owners(blob.Digest)
	require.Error(err)
}

func TestClusterClientGetMetaInfoSkipsOriginOnPollTimeout(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	b := backoff.New(backoff.Config{
		Min:          100 * time.Millisecond,
		RetryTimeout: 500 * time.Millisecond,
	})
	cc := blobclient.NewClusterClient(mockResolver, blobclient.WithPollMetaInfoBackoff(b))

	blob := core.NewBlobFixture()

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)

	mockResolver.EXPECT().Resolve(blob.Digest).Return([]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(nil, httputil.StatusError{Status: 202}).MinTimes(1)
	mockClient1.EXPECT().Addr().Return("client1")
	mockClient2.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(blob.MetaInfo, nil)

	result, err := cc.GetMetaInfo(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(result, blob.MetaInfo)
}

func TestClusterClientGetMetaInfoSkipsOriginOnNetworkErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	cc := blobclient.NewClusterClient(mockResolver)

	blob := core.NewBlobFixture()

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)

	mockResolver.EXPECT().Resolve(blob.Digest).Return([]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(nil, httputil.NetworkError{})
	mockClient1.EXPECT().Addr().Return("client1")
	mockClient2.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(blob.MetaInfo, nil)

	result, err := cc.GetMetaInfo(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(result, blob.MetaInfo)
}

func TestClusterClientReturnsErrorOnNoAvailableOrigins(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	cc := blobclient.NewClusterClient(mockResolver)

	blob := core.NewBlobFixture()

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)
	mockResolver.EXPECT().Resolve(blob.Digest).Return([]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(nil, httputil.NetworkError{})
	mockClient1.EXPECT().Addr().Return("client1")
	mockClient2.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(nil, httputil.NetworkError{})
	mockClient2.EXPECT().Addr().Return("client2")

	_, err := cc.GetMetaInfo(namespace, blob.Digest)
	require.Error(err)
}

func TestClusterClientOverwriteMetainfo(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	cc := blobclient.NewClusterClient(mockResolver)

	d := core.DigestFixture()

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)
	mockResolver.EXPECT().Resolve(d).Return([]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().OverwriteMetaInfo(d, int64(16)).Return(nil)
	mockClient2.EXPECT().OverwriteMetaInfo(d, int64(16)).Return(nil)

	err := cc.OverwriteMetaInfo(d, 16)
	require.NoError(err)
}
