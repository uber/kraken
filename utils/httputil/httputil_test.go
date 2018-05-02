package httputil

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/mocks/net/http"
	"code.uber.internal/infra/kraken/utils/backoff"
)

const _testURL = "http://localhost:0/test"

func newResponse(status int) *http.Response {
	// We need to set a dummy request in the response so NewStatusError
	// can access the "original" URL.
	dummyReq, err := http.NewRequest("GET", _testURL, nil)
	if err != nil {
		panic(err)
	}

	rec := httptest.NewRecorder()
	rec.WriteHeader(status)
	resp := rec.Result()
	resp.Request = dummyReq

	return resp
}

func TestSendRetry(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttp.NewMockRoundTripper(ctrl)

	for _, status := range []int{503, 500, 200} {
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(status), nil)
	}

	start := time.Now()
	_, err := Get(
		_testURL,
		SendRetry(RetryMax(5), RetryInterval(200*time.Millisecond)),
		SendTransport(transport))
	require.NoError(err)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestSendRetryOnTransportErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttp.NewMockRoundTripper(ctrl)

	transport.EXPECT().RoundTrip(gomock.Any()).Return(nil, errors.New("some network error")).Times(3)

	start := time.Now()
	_, err := Get(
		_testURL,
		SendRetry(RetryMax(3), RetryInterval(200*time.Millisecond)),
		SendTransport(transport))
	require.Error(err)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestSendRetryOn5XX(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttp.NewMockRoundTripper(ctrl)

	transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(503), nil).Times(3)

	start := time.Now()
	_, err := Get(
		_testURL,
		SendRetry(RetryMax(3), RetryInterval(200*time.Millisecond)),
		SendTransport(transport))
	require.Error(err)
	require.Equal(503, err.(StatusError).Status)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestPollAccepted(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttp.NewMockRoundTripper(ctrl)

	for _, status := range []int{202, 202, 200} {
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(status), nil)
	}

	start := time.Now()
	_, err := PollAccepted(
		_testURL,
		backoff.New(backoff.Config{Min: 200 * time.Millisecond, Factor: 1}),
		SendTransport(transport))
	require.NoError(err)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestPollAcceptedStatusError(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttp.NewMockRoundTripper(ctrl)

	for _, status := range []int{202, 202, 404} {
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(status), nil)
	}

	start := time.Now()
	_, err := PollAccepted(
		_testURL,
		backoff.New(backoff.Config{Min: 200 * time.Millisecond, Factor: 1}),
		SendTransport(transport))
	require.Error(err)
	require.Equal(404, err.(StatusError).Status)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}
