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
	"testing"

	"github.com/stretchr/testify/require"
)

// seriesValue returns the value of the series with the given name and labels.
func seriesValue(t *testing.T, r *prometheusReporter, name string, labels map[string]string) float64 {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.series[seriesKey(name, labels)]
	require.True(t, ok, "series %s %v not found", name, labels)
	return s.value
}

func TestPrometheusReporterCounterAccumulatesDeltas(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("requests", map[string]string{"code": "200"}, 2)
	r.ReportCounter("requests", map[string]string{"code": "200"}, 3)
	r.ReportCounter("requests", map[string]string{"code": "500"}, 1)

	require.Equal(t, 5.0, seriesValue(t, r, "requests", map[string]string{"code": "200"}))
	require.Equal(t, 1.0, seriesValue(t, r, "requests", map[string]string{"code": "500"}))
}

func TestPrometheusReporterGaugeKeepsLatestValue(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportGauge("queue_size", nil, 7)
	r.ReportGauge("queue_size", nil, 4)

	require.Equal(t, 4.0, seriesValue(t, r, "queue_size", nil))
}

func TestPrometheusReporterEmptyTagValueIsSameSeries(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("errors", map[string]string{"namespace": ""}, 1)
	r.ReportCounter("errors", nil, 1)

	require.Len(t, r.series, 1)
	require.Equal(t, 2.0, seriesValue(t, r, "errors", nil))
}

func TestPrometheusReporterDropsKindConflicts(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("conflict", nil, 1)
	r.ReportGauge("conflict", nil, 10)
	r.ReportGauge("conflict", nil, 20)

	require.Len(t, r.series, 1)
	require.Equal(t, _promCounter, r.kinds["conflict"])
	require.Equal(t, 1.0, seriesValue(t, r, "conflict", nil))
}

func TestPrometheusReporterSanitizesNames(t *testing.T) {
	r := newPrometheusReporter(nil)
	r.ReportCounter("peer.conn-errors", map[string]string{"remote.host": "a.b"}, 1)

	require.Equal(t, 1.0,
		seriesValue(t, r, "peer_conn_errors", map[string]string{"remote_host": "a.b"}))
}
