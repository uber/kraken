package httputil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/golang/mock/gomock"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/mocks/net/http"
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

func TestSendOptions(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttp.NewMockRoundTripper(ctrl)

	transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(499), nil)

	_, err := Get(
		_testURL,
		SendTransport(transport),
		SendAcceptedCodes(200, 499))
	require.NoError(err)
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
		SendRetry(
			RetryMax(5), RetryInterval(200*time.Millisecond),
			RetryBackoff(1), RetryBackoffMax(1*time.Second)),
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
		backoff.NewConstantBackOff(200*time.Millisecond),
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
		backoff.NewConstantBackOff(200*time.Millisecond),
		SendTransport(transport))
	require.Error(err)
	require.Equal(404, err.(StatusError).Status)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestPollAcceptedBackoffTimeout(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttp.NewMockRoundTripper(ctrl)

	transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(202), nil).Times(3)

	start := time.Now()
	_, err := PollAccepted(
		_testURL,
		backoff.WithMaxRetries(backoff.NewConstantBackOff(200*time.Millisecond), 2),
		SendTransport(transport))
	require.Error(err)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestGetQueryArg(t *testing.T) {
	require := require.New(t)
	arg := "arg"
	value := "value"
	defaultVal := "defaultvalue"

	r := httptest.NewRequest("GET", fmt.Sprintf("localhost:0/?%s=%s", arg, value), nil)
	require.Equal(value, GetQueryArg(r, arg, defaultVal))
}

func TestGetQueryArgUseDefault(t *testing.T) {
	require := require.New(t)
	arg := "arg"
	defaultVal := "defaultvalue"

	r := httptest.NewRequest("GET", "localhost:0/", nil)
	require.Equal(defaultVal, GetQueryArg(r, arg, defaultVal))
}

func TestParseParam(t *testing.T) {
	require := require.New(t)

	r := httptest.NewRequest("GET", "/", nil)

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("key", "a%2Fb")

	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	ret, err := ParseParam(r, "key")
	require.NoError(err)
	require.Equal("a/b", ret)
}

func TestParseParamNotFound(t *testing.T) {
	require := require.New(t)

	r := httptest.NewRequest("GET", "/", nil)
	rctx := chi.NewRouteContext()

	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	_, err := ParseParam(r, "key")
	require.Error(err)
}

func TestParseParamUnescapeError(t *testing.T) {
	require := require.New(t)

	r := httptest.NewRequest("GET", "/", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("key", "value%")

	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	_, err := ParseParam(r, "key")
	require.Error(err)
}

func TestParseDigest(t *testing.T) {
	require := require.New(t)

	r := httptest.NewRequest("GET", "/", nil)

	d := core.DigestFixture()
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("digest", d.String())

	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	ret, err := ParseDigest(r, "digest")
	require.NoError(err)
	require.Equal(d, ret)
}

func TestParseDigestInvalid(t *testing.T) {
	require := require.New(t)

	r := httptest.NewRequest("GET", "/", nil)

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("digest", "abc")

	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	_, err := ParseDigest(r, "digest")
	require.Error(err)
}
