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
package metrics

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestPrometheusConfigDefaults(t *testing.T) {
	c := PrometheusConfig{ListenAddress: ":9091"}.applyDefaults()
	require.Equal(t, "/metrics", c.HandlerPath)
	require.Equal(t, "kraken", c.Prefix)
	require.Equal(t, _defaultPrometheusTimerBuckets, c.TimerBuckets)
	require.NoError(t, c.validate())

	c = PrometheusConfig{TimerBuckets: []float64{1, 2}}.applyDefaults()
	require.Equal(t, []float64{1, 2}, c.TimerBuckets)
}

func TestPrometheusConfigValidation(t *testing.T) {
	tests := []struct {
		desc   string
		config PrometheusConfig
		errMsg string
	}{
		{"missing listen address", PrometheusConfig{}, "listen_address required"},
		{
			"bad handler path",
			PrometheusConfig{ListenAddress: "127.0.0.1:0", HandlerPath: "metrics"},
			"must start with /",
		},
		{
			"unsorted buckets",
			PrometheusConfig{ListenAddress: "127.0.0.1:0", TimerBuckets: []float64{1, 0.5}},
			"must be sorted",
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			err := test.config.applyDefaults().validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), test.errMsg)
		})
	}
}

func TestPrometheusBackendRejectsInvalidConfig(t *testing.T) {
	_, _, err := New(Config{Backend: "prometheus"}, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "listen_address required")
}

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

func TestPrometheusScopeServesMetricsEndToEnd(t *testing.T) {
	require := require.New(t)

	config := Config{
		Backend: "prometheus",
		Prometheus: PrometheusConfig{
			ListenAddress: "127.0.0.1:0",
			HandlerPath:   "/custom",
		},
	}
	scope, closer, addr, err := newPrometheusScopeWithAddr(config)
	require.NoError(err)

	origin := scope.Tagged(map[string]string{"module": "blobserver"})
	origin.Counter("upload_success").Inc(3)
	origin.Gauge("inflight_uploads").Update(2)
	origin.Timer("commit_upload").Record(30 * time.Millisecond)
	scope.Histogram("piece_size", tally.ValueBuckets{1024, 4096}).RecordValue(2000)

	url := fmt.Sprintf("http://%s/custom", addr)
	var body string
	require.Eventually(func() bool {
		body = scrape(t, url)
		return strings.Contains(body, "kraken_piece_size_count")
	}, 5*time.Second, 50*time.Millisecond)

	require.Contains(body, `kraken_upload_success{module="blobserver"} 3`)
	require.Contains(body, `kraken_inflight_uploads{module="blobserver"} 2`)
	require.Contains(body, `kraken_commit_upload_bucket{module="blobserver",le="0.032"} 1`)
	require.Contains(body, `kraken_commit_upload_count{module="blobserver"} 1`)
	require.Contains(body, `kraken_piece_size_bucket{le="4096"} 1`)
	// Go runtime and process metrics are exported alongside Kraken's.
	require.Contains(body, "go_goroutines")

	// Metrics are only served on the configured path.
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
	require.NoError(err)
	require.NoError(resp.Body.Close())
	require.Equal(http.StatusNotFound, resp.StatusCode)

	// Closing the scope shuts down the server and frees the port.
	require.NoError(closer.Close())
	_, err = net.DialTimeout("tcp", addr.String(), time.Second)
	require.Error(err)
	l, err := net.Listen("tcp", addr.String())
	require.NoError(err)
	require.NoError(l.Close())
}

func TestPrometheusScopeFailsWhenAddressInUse(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, l.Close()) }()

	_, _, err = New(Config{
		Backend:    "prometheus",
		Prometheus: PrometheusConfig{ListenAddress: l.Addr().String()},
	}, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "listen on")
}
