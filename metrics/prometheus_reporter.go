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
	"sync"

	"github.com/uber/kraken/utils/log"
)

type promKind int

const (
	_promCounter promKind = iota
	_promGauge
	_promHistogram
)

func (k promKind) String() string {
	switch k {
	case _promCounter:
		return "counter"
	case _promGauge:
		return "gauge"
	default:
		return "histogram"
	}
}

// promSeries holds the current state of a single time series.
type promSeries struct {
	name   string
	labels map[string]string
	value  float64
}

// prometheusReporter is a tally.StatsReporter that keeps the latest value of
// every series in memory.
//
// A metric name maps to exactly one kind. If tally reports the same name as a
// different kind, those reports are dropped and a warning is logged once.
type prometheusReporter struct {
	mu        sync.Mutex
	series    map[string]*promSeries
	kinds     map[string]promKind
	conflicts map[string]bool
}

func newPrometheusReporter() *prometheusReporter {
	return &prometheusReporter{
		series:    make(map[string]*promSeries),
		kinds:     make(map[string]promKind),
		conflicts: make(map[string]bool),
	}
}

// getSeries returns the series for name and tags, creating it if needed. It
// returns nil if name is already used by a metric of a different kind. The
// caller must hold r.mu.
func (r *prometheusReporter) getSeries(
	name string, tags map[string]string, kind promKind,
) *promSeries {
	name = sanitizePrometheusName(name)
	if k, ok := r.kinds[name]; ok && k != kind {
		if !r.conflicts[name] {
			r.conflicts[name] = true
			log.With("name", name, "kind", kind, "existing_kind", k).
				Warn("Dropping metric whose name is already used by another metric type")
		}
		return nil
	}
	r.kinds[name] = kind

	labels := make(map[string]string, len(tags))
	for k, v := range tags {
		// An empty label value is the same as no label in Prometheus.
		if v != "" {
			labels[sanitizePrometheusName(k)] = v
		}
	}
	key := seriesKey(name, labels)
	s, ok := r.series[key]
	if !ok {
		s = &promSeries{name: name, labels: labels}
		r.series[key] = s
	}
	return s
}

// ReportCounter implements tally.StatsReporter. value is the delta since the
// last report.
func (r *prometheusReporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s := r.getSeries(name, tags, _promCounter); s != nil {
		s.value += float64(value)
	}
}

// ReportGauge implements tally.StatsReporter.
func (r *prometheusReporter) ReportGauge(name string, tags map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s := r.getSeries(name, tags, _promGauge); s != nil {
		s.value = value
	}
}
