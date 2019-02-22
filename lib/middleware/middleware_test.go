// Copyright (c) 2019 Uber Technologies, Inc.
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
package middleware

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/testutil"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestScopeByEndpoint(t *testing.T) {
	tests := []struct {
		method           string
		path             string
		reqPath          string
		expectedEndpoint string
	}{
		{"GET", "/foo/:foo/bar/:bar", "/foo/x/bar/y", "foo.bar"},
		{"POST", "/foo/:foo/bar/:bar", "/foo/x/bar/y", "foo.bar"},
		{"GET", "/a/b/c", "/a/b/c", "a.b.c"},
		{"GET", "/", "/", ""},
		{"GET", "/x/:a/:b/:c", "/x/a/b/c", "x"},
	}

	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			require := require.New(t)

			stats := tally.NewTestScope("", nil)

			r := chi.NewRouter()
			r.HandleFunc(test.path, func(w http.ResponseWriter, r *http.Request) {
				tagEndpoint(stats, r).Counter("count").Inc(1)
			})
			addr, stop := testutil.StartServer(r)
			defer stop()

			_, err := httputil.Send(test.method, fmt.Sprintf("http://%s%s", addr, test.reqPath))
			require.NoError(err)

			counter, ok := stats.Snapshot().Counters()["count"]
			require.True(ok)
			require.Equal(map[string]string{
				"endpoint": test.expectedEndpoint,
				"method":   test.method,
			}, counter.Tags())
			require.Equal(int64(1), counter.Value())
		})
	}
}

func TestLatencyTimer(t *testing.T) {
	require := require.New(t)

	stats := tally.NewTestScope("", nil)

	r := chi.NewRouter()
	r.Use(LatencyTimer(stats))
	r.Get("/foo/:foo", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	})

	addr, stop := testutil.StartServer(r)
	defer stop()

	_, err := httputil.Get(fmt.Sprintf("http://%s/foo/x", addr))
	require.NoError(err)

	now := time.Now()

	timer, ok := stats.Snapshot().Timers()["latency"]
	require.True(ok)
	require.WithinDuration(now, now.Add(timer.Values()[0]), 500*time.Millisecond)
	require.Equal(map[string]string{
		"endpoint": "foo",
		"method":   "GET",
	}, timer.Tags())
}

func TestStatusCounter(t *testing.T) {
	tests := []struct {
		desc           string
		handler        func(http.ResponseWriter, *http.Request)
		expectedStatus string
	}{
		{
			"empty handler counts 200",
			func(http.ResponseWriter, *http.Request) {},
			"200",
		}, {
			"writes count 200",
			func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "OK") },
			"200",
		}, {
			"write header",
			func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) },
			"500",
		}, {
			"multiple write header calls only measures first call",
			func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(400); w.WriteHeader(500) },
			"400",
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			stats := tally.NewTestScope("", nil)

			r := chi.NewRouter()
			r.Use(StatusCounter(stats))
			r.Get("/foo/:foo", test.handler)

			addr, stop := testutil.StartServer(r)
			defer stop()

			for i := 0; i < 5; i++ {
				_, err := http.Get(fmt.Sprintf("http://%s/foo/x", addr))
				require.NoError(err)
			}

			counter, ok := stats.Snapshot().Counters()[test.expectedStatus]
			require.True(ok)
			require.Equal(int64(5), counter.Value())
			require.Equal(map[string]string{
				"endpoint": "foo",
				"method":   "GET",
			}, counter.Tags())
		})
	}
}
