// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
	"github.com/go-chi/chi"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	mockhttputil "github.com/uber/kraken/mocks/utils/httputil"
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

	transport := mockhttputil.NewMockRoundTripper(ctrl)

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

	transport := mockhttputil.NewMockRoundTripper(ctrl)

	for _, status := range []int{503, 502, 200} {
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(status), nil)
	}

	start := time.Now()
	_, err := Get(
		_testURL,
		SendRetry(
			RetryBackoff(backoff.WithMaxRetries(
				backoff.NewConstantBackOff(200*time.Millisecond),
				4))),
		SendTransport(transport))
	require.NoError(err)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestSendRetryOnTransportErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttputil.NewMockRoundTripper(ctrl)

	transport.EXPECT().RoundTrip(gomock.Any()).Return(nil, errors.New("some network error")).Times(3)

	start := time.Now()
	_, err := Get(
		_testURL,
		SendRetry(
			RetryBackoff(backoff.WithMaxRetries(
				backoff.NewConstantBackOff(200*time.Millisecond),
				2))),
		SendTransport(transport))
	require.Error(err)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestSendRetryOn5XX(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttputil.NewMockRoundTripper(ctrl)

	transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(503), nil).Times(3)

	start := time.Now()
	_, err := Get(
		_testURL,
		SendRetry(
			RetryBackoff(backoff.WithMaxRetries(
				backoff.NewConstantBackOff(200*time.Millisecond),
				2))),
		SendTransport(transport))
	require.Error(err)
	statusErr, ok := err.(StatusError)
	require.True(ok, "expected StatusError")
	require.Equal(503, statusErr.Status)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestSendRetryWithCodes(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttputil.NewMockRoundTripper(ctrl)

	gomock.InOrder(
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(400), nil),
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(503), nil),
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(404), nil),
	)

	start := time.Now()
	_, err := Get(
		_testURL,
		SendRetry(
			RetryBackoff(backoff.WithMaxRetries(
				backoff.NewConstantBackOff(200*time.Millisecond),
				2)),
			RetryCodes(400, 404)),
		SendTransport(transport))
	require.Error(err)
	statusErr, ok := err.(StatusError)
	require.True(ok)
	require.Equal(404, statusErr.Status) // Last code returned.
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestPollAccepted(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttputil.NewMockRoundTripper(ctrl)

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

	transport := mockhttputil.NewMockRoundTripper(ctrl)

	for _, status := range []int{202, 202, 404} {
		transport.EXPECT().RoundTrip(gomock.Any()).Return(newResponse(status), nil)
	}

	start := time.Now()
	_, err := PollAccepted(
		_testURL,
		backoff.NewConstantBackOff(200*time.Millisecond),
		SendTransport(transport))
	require.Error(err)
	statusErr, ok := err.(StatusError)
	require.True(ok)
	require.Equal(404, statusErr.Status)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestPollAcceptedBackoffTimeout(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transport := mockhttputil.NewMockRoundTripper(ctrl)

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

func TestSendRetryResetsExponentialBackoff(t *testing.T) {
	require := require.New(t)

	// A backoff built the way production builds it: ExponentialBackOffConfig.Build()
	// returns an ExponentialBackOff whose currentInterval is still 0.
	b := ExponentialBackOffConfig{
		Enabled:             true,
		InitialInterval:     50 * time.Millisecond,
		Multiplier:          1,
		MaxInterval:         50 * time.Millisecond,
		RandomizationFactor: 0.05,
		MaxRetries:          2,
	}.Build()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	start := time.Now()
	_, err := Get(srv.URL, SendRetry(RetryBackoff(b)))
	require.Error(err)
	// Two retries at ~50ms each. Without the Reset in Send both sleeps are ~0.
	require.GreaterOrEqual(time.Since(start), 80*time.Millisecond)
}

func TestSendRetriesReuseBackoff(t *testing.T) {
	require := require.New(t)

	// One backoff reused by two independent Send calls. Resetting only when the
	// config is built would fix the first call and leave the second sleeping for
	// ~0, because the shared backoff is already exhausted; the reset has to run
	// at the start of every Send.
	b := ExponentialBackOffConfig{
		Enabled:             true,
		InitialInterval:     50 * time.Millisecond,
		Multiplier:          1,
		MaxInterval:         50 * time.Millisecond,
		RandomizationFactor: 0.05,
		MaxRetries:          2,
	}.Build()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	var elapsed [2]time.Duration
	for i := range elapsed {
		start := time.Now()
		_, err := Get(srv.URL, SendRetry(RetryBackoff(b)))
		require.Error(err)
		elapsed[i] = time.Since(start)
	}

	// Two retries at ~50ms each on every call. Without the reset in Send the
	// reused backoff is already exhausted on the second call, so it returns
	// backoff.Stop immediately and elapsed[1] stays near zero.
	require.GreaterOrEqual(elapsed[0], 80*time.Millisecond)
	require.GreaterOrEqual(elapsed[1], 80*time.Millisecond)
}
