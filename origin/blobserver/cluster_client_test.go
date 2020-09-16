// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package blobserver

import (
	"bytes"
	"io/ioutil"
	"sort"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/httputil"

	"github.com/cenkalti/backoff"
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

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	// Register dummy master addresses so Provide can still create a Client for
	// unavailable masters.
	cp.register(master2, blobclient.New("localhost:0"))
	cp.register(master3, blobclient.New("localhost:0"))

	r := blobclient.NewClientResolver(cp, hostlist.Fixture(master1))
	cc := blobclient.NewClusterClient(r)

	// Run many times to make sure we eventually hit unavailable masters.
	for i := 0; i < 100; i++ {
		blob := core.SizedBlobFixture(256, 8)

		s.writeBackManager.EXPECT().Add(
			writeback.MatchTask(writeback.NewTask(
				backend.NoopNamespace, blob.Digest.Hex(), 0))).Return(nil)
		require.NoError(cc.UploadBlob(backend.NoopNamespace, blob.Digest, bytes.NewReader(blob.Content)))

		bi, err := cc.Stat(backend.NoopNamespace, blob.Digest)
		require.NoError(err)
		require.NotNil(bi)
		require.Equal(int64(256), bi.Size)

		mi, err := cc.GetMetaInfo(backend.NoopNamespace, blob.Digest)
		require.NoError(err)
		require.NotNil(mi)

		var buf bytes.Buffer
		require.NoError(cc.DownloadBlob(backend.NoopNamespace, blob.Digest, &buf))
		require.Equal(string(blob.Content), buf.String())

		peers, err := cc.Owners(blob.Digest)
		require.NoError(err)
		require.Len(peers, 1)
		require.Equal(s.pctx, peers[0])
	}
}

func TestClusterClientReturnsErrorOnNoAvailability(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()
	cp.register(master1, blobclient.New("localhost:0"))
	cp.register(master2, blobclient.New("localhost:0"))
	cp.register(master3, blobclient.New("localhost:0"))

	r := blobclient.NewClientResolver(cp, hostlist.Fixture(master1))
	cc := blobclient.NewClusterClient(r)

	blob := core.NewBlobFixture()

	require.Error(cc.UploadBlob(backend.NoopNamespace, blob.Digest, bytes.NewReader(blob.Content)))

	_, err := cc.Stat(backend.NoopNamespace, blob.Digest)
	require.Error(err)

	_, err = cc.GetMetaInfo(backend.NoopNamespace, blob.Digest)
	require.Error(err)

	require.Error(cc.DownloadBlob(backend.NoopNamespace, blob.Digest, ioutil.Discard))

	_, err = cc.Owners(blob.Digest)
	require.Error(err)
}

func TestPollSkipsOriginOnTimeout(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)

	mockResolver.EXPECT().Resolve(blob.Digest).Return(
		[]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().DownloadBlob(
		namespace, blob.Digest, nil).Return(httputil.StatusError{Status: 202}).MinTimes(1)
	mockClient1.EXPECT().Addr().Return("client1")
	mockClient2.EXPECT().DownloadBlob(namespace, blob.Digest, nil).Return(nil)

	b := backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 5)

	require.NoError(blobclient.Poll(mockResolver, b, blob.Digest, func(c blobclient.Client) error {
		return c.DownloadBlob(namespace, blob.Digest, nil)
	}))
}

func TestPollSkipsOriginOnNetworkErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)

	mockResolver.EXPECT().Resolve(blob.Digest).Return([]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().DownloadBlob(namespace, blob.Digest, nil).Return(httputil.NetworkError{})
	mockClient1.EXPECT().Addr().Return("client1")
	mockClient2.EXPECT().DownloadBlob(namespace, blob.Digest, nil).Return(nil)

	b := backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 5)

	require.NoError(blobclient.Poll(mockResolver, b, blob.Digest, func(c blobclient.Client) error {
		return c.DownloadBlob(namespace, blob.Digest, nil)
	}))
}

func TestPollSkipsOriginOnRetryableError(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)
	cc := blobclient.NewClusterClient(mockResolver)

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)

	mockResolver.EXPECT().Resolve(blob.Digest).Return([]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().UploadBlob(namespace, blob.Digest, nil).Return(httputil.StatusError{Status: 503})
	mockClient2.EXPECT().UploadBlob(namespace, blob.Digest, nil).Return(nil)

	require.NoError(cc.UploadBlob(namespace, blob.Digest, nil))
}

func TestClusterClientReturnsErrorOnNoAvailableOrigins(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	cc := blobclient.NewClusterClient(mockResolver)

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)
	mockResolver.EXPECT().Resolve(blob.Digest).Return([]blobclient.Client{mockClient1, mockClient2}, nil)

	mockClient1.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(nil, httputil.NetworkError{})
	mockClient2.EXPECT().GetMetaInfo(namespace, blob.Digest).Return(nil, httputil.NetworkError{})

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

func TestClusterClientStatContinueWhenNotFound(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mockblobclient.NewMockClientResolver(ctrl)

	cc := blobclient.NewClusterClient(mockResolver)

	blob := core.SizedBlobFixture(256, 8)
	namespace := core.TagFixture()

	mockClient := mockblobclient.NewMockClient(ctrl)
	// Reuse the same mockClient for two origins because origins are shuffled.
	mockResolver.EXPECT().Resolve(blob.Digest).Return([]blobclient.Client{mockClient, mockClient}, nil)

	gomock.InOrder(
		mockClient.EXPECT().Stat(namespace, blob.Digest).Return(nil, blobclient.ErrBlobNotFound),
		mockClient.EXPECT().Stat(namespace, blob.Digest).Return(core.NewBlobInfo(256), nil),
	)

	bi, err := cc.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.NotNil(bi)
	require.Equal(int64(256), bi.Size)
}
