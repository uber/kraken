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
	"github.com/go-chi/chi"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/mocks/utils/httputil"
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
	require.Equal(503, err.(StatusError).Status)
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
	require.Equal(404, err.(StatusError).Status) // Last code returned.
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
	require.Equal(404, err.(StatusError).Status)
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
