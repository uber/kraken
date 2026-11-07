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
