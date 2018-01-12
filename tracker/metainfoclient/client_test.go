package metainfoclient_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/mocks/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const namespace = "test-namespace"

func getURL(addr string, d image.Digest) string {
	return fmt.Sprintf("http://%s/namespace/%s/blobs/%s/metainfo", addr, namespace, d)
}

func TestDownloadTriesAllServersOnNetworkErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	addrs := []string{"t1", "t2", "t3"}
	servers, err := serverset.NewRoundRobin(serverset.RoundRobinConfig{
		Addrs:   addrs,
		Retries: 3,
	})
	require.NoError(err)
	mockGetter := mockmetainfoclient.NewMockGetter(ctrl)

	client := metainfoclient.New(
		metainfoclient.Config{},
		servers,
		metainfoclient.WithGetter(mockGetter))

	d := image.DigestFixture()

	for _, addr := range addrs {
		mockGetter.EXPECT().Get(getURL(addr, d)).Return(nil, errors.New("some network err"))
	}

	_, err = client.Download(namespace, d.Hex())
	require.Error(err)
	require.IsType(serverset.MaxRoundRobinRetryError{}, err)
}

func TestDownloadPollsOnStatusAcceptedUntilStatusOK(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGetter := mockmetainfoclient.NewMockGetter(ctrl)

	config := metainfoclient.Config{
		PollBackoff: backoff.Config{
			Min:          1 * time.Millisecond,
			Max:          1 * time.Millisecond,
			RetryTimeout: 15 * time.Second,
		},
	}

	mi := torlib.MetaInfoFixture()
	d := image.NewSHA256DigestFromHex(mi.Name())
	miRaw, err := mi.Serialize()
	require.NoError(err)

	addr := "test-tracker"
	client := metainfoclient.New(
		config, serverset.NewSingle(addr), metainfoclient.WithGetter(mockGetter))

	url := getURL(addr, d)
	gomock.InOrder(
		mockGetter.EXPECT().Get(url).Return(&http.Response{StatusCode: 202}, nil),
		mockGetter.EXPECT().Get(url).Return(&http.Response{StatusCode: 202}, nil),
		mockGetter.EXPECT().Get(url).Return(
			&http.Response{StatusCode: 200, Body: ioutil.NopCloser(bytes.NewReader(miRaw))}, nil),
	)

	result, err := client.Download(namespace, d.Hex())
	require.NoError(err)
	require.Equal(mi, result)
}

func TestDownloadPollTimeout(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGetter := mockmetainfoclient.NewMockGetter(ctrl)

	config := metainfoclient.Config{
		PollBackoff: backoff.Config{
			Min:          10 * time.Millisecond,
			RetryTimeout: 1 * time.Second,
		},
	}

	d := image.DigestFixture()

	addr := "test-tracker"
	client := metainfoclient.New(
		config, serverset.NewSingle(addr), metainfoclient.WithGetter(mockGetter))

	mockGetter.EXPECT().Get(getURL(addr, d)).Return(&http.Response{StatusCode: 202}, nil).MinTimes(1)

	_, err := client.Download(namespace, d.Hex())
	require.Error(err)
	require.True(backoff.IsTimeoutError(err))
}

func TestDownloadConverts404ToErrNotFound(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGetter := mockmetainfoclient.NewMockGetter(ctrl)

	d := image.DigestFixture()

	addr := "test-tracker"
	client := metainfoclient.New(
		metainfoclient.Config{}, serverset.NewSingle(addr), metainfoclient.WithGetter(mockGetter))

	mockGetter.EXPECT().Get(getURL(addr, d)).Return(&http.Response{StatusCode: 404}, nil)

	_, err := client.Download(namespace, d.Hex())
	require.Equal(metainfoclient.ErrNotFound, err)
}

func TestDownloadPropagatesStatusError(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGetter := mockmetainfoclient.NewMockGetter(ctrl)

	d := image.DigestFixture()

	addr := "test-tracker"
	client := metainfoclient.New(
		metainfoclient.Config{}, serverset.NewSingle(addr), metainfoclient.WithGetter(mockGetter))

	url := getURL(addr, d)
	mockGetter.EXPECT().Get(url).Return(
		&http.Response{
			Request:    httptest.NewRequest("GET", url, nil),
			StatusCode: 599,
			Body:       ioutil.NopCloser(bytes.NewBufferString("some error")),
		}, nil)

	_, err := client.Download(namespace, d.Hex())
	require.Error(err)
	require.True(httputil.IsStatus(err, 599))
}
