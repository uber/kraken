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
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

var _ prometheus.Collector = (*prometheusReporter)(nil)

// Describe implements prometheus.Collector. It sends no descriptors, which
// makes the reporter an unchecked collector: the set of metrics is only known
// once tally reports them.
func (r *prometheusReporter) Describe(chan<- *prometheus.Desc) {}

// Collect implements prometheus.Collector.
func (r *prometheusReporter) Collect(ch chan<- prometheus.Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Group series by metric name and compute the union of label names.
	families := make(map[string][]*promSeries)
	labelNames := make(map[string]map[string]struct{})
	for _, s := range r.series {
		families[s.name] = append(families[s.name], s)
		names, ok := labelNames[s.name]
		if !ok {
			names = make(map[string]struct{})
			labelNames[s.name] = names
		}
		for k := range s.labels {
			names[k] = struct{}{}
		}
	}

	for name, series := range families {
		keys := make([]string, 0, len(labelNames[name]))
		for k := range labelNames[name] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		kind := r.kinds[name]
		desc := prometheus.NewDesc(name, fmt.Sprintf("Kraken %s %s.", kind, name), keys, nil)

		for _, s := range series {
			values := make([]string, len(keys))
			for i, k := range keys {
				values[i] = s.labels[k]
			}
			var (
				m   prometheus.Metric
				err error
			)
			switch kind {
			case _promCounter:
				m, err = prometheus.NewConstMetric(
					desc, prometheus.CounterValue, s.value, values...)
			case _promGauge:
				m, err = prometheus.NewConstMetric(
					desc, prometheus.GaugeValue, s.value, values...)
			default:
				m, err = prometheus.NewConstHistogram(
					desc, s.count, s.sum, s.cumulativeBuckets(), values...)
			}
			if err != nil {
				m = prometheus.NewInvalidMetric(desc, err)
			}
			ch <- m
		}
	}
}

func (s *promSeries) cumulativeBuckets() map[float64]uint64 {
	buckets := make(map[float64]uint64, len(s.upperBounds))
	var total uint64
	for i, b := range s.upperBounds {
		total += s.counts[i]
		buckets[b] = total
	}
	return buckets
}
