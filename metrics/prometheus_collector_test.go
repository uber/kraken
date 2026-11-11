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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func gatherText(t *testing.T, r *prometheusReporter, expected string, names ...string) {
	t.Helper()
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(r))
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expected), names...))
}

func TestPrometheusCollectorCounterAccumulatesDeltas(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("requests", map[string]string{"code": "200"}, 2)
	r.ReportCounter("requests", map[string]string{"code": "200"}, 3)
	r.ReportCounter("requests", map[string]string{"code": "500"}, 1)

	gatherText(t, r, `
# HELP requests Kraken counter requests.
# TYPE requests counter
requests{code="200"} 5
requests{code="500"} 1
`, "requests")
}

func TestPrometheusCollectorGaugeKeepsLatestValue(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportGauge("queue_size", nil, 7)
	r.ReportGauge("queue_size", nil, 4)

	gatherText(t, r, `
# HELP queue_size Kraken gauge queue_size.
# TYPE queue_size gauge
queue_size 4
`, "queue_size")
}

func TestPrometheusCollectorPadsInconsistentLabels(t *testing.T) {
	// The same metric emitted from scopes with different tags must not be
	// dropped: missing labels are exported as empty, which Prometheus treats
	// as absent.
	r := newPrometheusReporter(nil)
	r.ReportCounter("blob_download", map[string]string{"module": "castore"}, 1)
	r.ReportCounter("blob_download", map[string]string{"module": "agent", "result": "hit"}, 2)
	r.ReportCounter("blob_download", nil, 3)

	gatherText(t, r, `
# HELP blob_download Kraken counter blob_download.
# TYPE blob_download counter
blob_download{module="",result=""} 3
blob_download{module="agent",result="hit"} 2
blob_download{module="castore",result=""} 1
`, "blob_download")
}

func TestPrometheusCollectorEmptyTagValueIsSameSeries(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("errors", map[string]string{"namespace": ""}, 1)
	r.ReportCounter("errors", nil, 1)

	gatherText(t, r, `
# HELP errors Kraken counter errors.
# TYPE errors counter
errors 2
`, "errors")
}

func TestPrometheusCollectorDropsKindConflicts(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("conflict", nil, 1)
	r.ReportGauge("conflict", nil, 10)
	r.ReportTimer("conflict", nil, time.Second)

	gatherText(t, r, `
# HELP conflict Kraken counter conflict.
# TYPE conflict counter
conflict 1
`, "conflict")
}

func TestPrometheusCollectorSanitizesNames(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("peer.conn-errors", map[string]string{"remote.host": "a.b"}, 1)

	gatherText(t, r, `
# HELP peer_conn_errors Kraken counter peer_conn_errors.
# TYPE peer_conn_errors counter
peer_conn_errors{remote_host="a.b"} 1
`, "peer_conn_errors")
}

func TestPrometheusCollectorTimerIsHistogramInSeconds(t *testing.T) {
	r := newPrometheusReporter([]float64{0.1, 1})
	r.ReportTimer("latency", nil, 50*time.Millisecond)
	r.ReportTimer("latency", nil, 500*time.Millisecond)
	r.ReportTimer("latency", nil, 2*time.Second)

	gatherText(t, r, `
# HELP latency Kraken histogram latency.
# TYPE latency histogram
latency_bucket{le="0.1"} 1
latency_bucket{le="1"} 2
latency_bucket{le="+Inf"} 3
latency_sum 2.55
latency_count 3
`, "latency")
}

func TestPrometheusCollectorValueHistogramSamples(t *testing.T) {
	r := newPrometheusReporter(nil)
	buckets := tally.ValueBuckets{10, 20}
	for _, pair := range tally.BucketPairs(buckets) {
		var samples int64
		switch {
		case pair.UpperBoundValue() == 10:
			samples = 1
		case pair.UpperBoundValue() == 20:
			samples = 2
		case pair.UpperBoundValue() == math.MaxFloat64:
			samples = 4
		}
		r.ReportHistogramValueSamples(
			"blob_size", nil, buckets,
			pair.LowerBoundValue(), pair.UpperBoundValue(), samples)
	}

	// The sum is estimated from bucket bounds: 1*10 + 2*20 + 4*20.
	gatherText(t, r, `
# HELP blob_size Kraken histogram blob_size.
# TYPE blob_size histogram
blob_size_bucket{le="10"} 1
blob_size_bucket{le="20"} 3
blob_size_bucket{le="+Inf"} 7
blob_size_sum 130
blob_size_count 7
`, "blob_size")
}

func TestPrometheusCollectorDurationHistogramSamples(t *testing.T) {
	r := newPrometheusReporter(nil)
	buckets := tally.DurationBuckets{0, 100 * time.Millisecond, time.Second}
	for _, pair := range tally.BucketPairs(buckets) {
		var samples int64
		switch pair.UpperBoundDuration() {
		case 100 * time.Millisecond:
			samples = 2
		case time.Second:
			samples = 1
		}
		r.ReportHistogramDurationSamples(
			"drain_duration", map[string]string{"module": "castore"}, buckets,
			pair.LowerBoundDuration(), pair.UpperBoundDuration(), samples)
	}

	gatherText(t, r, `
# HELP drain_duration Kraken histogram drain_duration.
# TYPE drain_duration histogram
drain_duration_bucket{module="castore",le="0"} 0
drain_duration_bucket{module="castore",le="0.1"} 2
drain_duration_bucket{module="castore",le="1"} 3
drain_duration_bucket{module="castore",le="+Inf"} 3
drain_duration_sum{module="castore"} 1.2
drain_duration_count{module="castore"} 3
`, "drain_duration")
}
