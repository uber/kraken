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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestPrometheusReporterTimerIsHistogramInSeconds(t *testing.T) {
	require := require.New(t)

	r := newPrometheusReporter([]float64{0.1, 1})
	r.ReportTimer("latency", nil, 50*time.Millisecond)
	r.ReportTimer("latency", nil, 500*time.Millisecond)
	r.ReportTimer("latency", nil, 2*time.Second)

	s := r.series[seriesKey("latency", nil)]
	require.Equal([]float64{0.1, 1}, s.upperBounds)
	require.Equal([]uint64{1, 1, 1}, s.counts)
	require.Equal(uint64(3), s.count)
	require.InDelta(2.55, s.sum, 1e-9)
}

func TestPrometheusReporterTimerKindConflict(t *testing.T) {
	r := newPrometheusReporter([]float64{1})
	r.ReportCounter("conflict", nil, 1)
	r.ReportTimer("conflict", nil, time.Second)

	require.Equal(t, _promCounter, r.kinds["conflict"])
	require.Nil(t, r.series[seriesKey("conflict", nil)].counts)
}

func TestPromSeriesInitBucketsSortsAndDropsInfinite(t *testing.T) {
	var s promSeries
	s.initBuckets([]float64{5, 1, math.Inf(1), 1, -math.MaxFloat64, math.MaxFloat64})
	require.Equal(t, []float64{1, 5}, s.upperBounds)
	require.Len(t, s.counts, 3)
}

func TestPrometheusReporterValueHistogramSamples(t *testing.T) {
	require := require.New(t)

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

	s := r.series[seriesKey("blob_size", nil)]
	require.Equal([]float64{10, 20}, s.upperBounds)
	require.Equal([]uint64{1, 2, 4}, s.counts)
	require.Equal(uint64(7), s.count)
	// The sum is estimated from bucket bounds: 1*10 + 2*20 + 4*20.
	require.InDelta(130.0, s.sum, 1e-9)
}

func TestPrometheusReporterDurationHistogramSamples(t *testing.T) {
	require := require.New(t)

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

	s := r.series[seriesKey("drain_duration", map[string]string{"module": "castore"})]
	require.Equal([]float64{0, 0.1, 1}, s.upperBounds)
	require.Equal([]uint64{0, 2, 1, 0}, s.counts)
	require.Equal(uint64(3), s.count)
	require.InDelta(1.2, s.sum, 1e-9)
}

func TestPrometheusReporterWithTallyScope(t *testing.T) {
	require := require.New(t)

	r := newPrometheusReporter([]float64{0.1})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:    "kraken",
		Separator: "_",
		Reporter:  r,
	}, time.Hour)
	tagged := scope.Tagged(map[string]string{"module": "blobserver"})
	tagged.Counter("upload_success").Inc(3)
	tagged.Gauge("inflight_uploads").Update(2)
	tagged.Timer("commit_upload").Record(30 * time.Millisecond)
	require.NoError(closer.Close()) // Close flushes pending reports.

	labels := map[string]string{"module": "blobserver"}
	require.Equal(3.0, seriesValue(t, r, "kraken_upload_success", labels))
	require.Equal(2.0, seriesValue(t, r, "kraken_inflight_uploads", labels))
	require.Equal([]uint64{1, 0}, r.series[seriesKey("kraken_commit_upload", labels)].counts)
}
