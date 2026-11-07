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
	"sort"
	"time"

	"github.com/uber-go/tally"
)

var _ tally.StatsReporter = (*prometheusReporter)(nil)

// ReportTimer implements tally.StatsReporter. Timers are exported as
// histograms in seconds.
func (r *prometheusReporter) ReportTimer(
	name string, tags map[string]string, interval time.Duration,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.getSeries(name, tags, _promHistogram)
	if s == nil {
		return
	}
	if s.counts == nil {
		s.initBuckets(r.timerBuckets)
	}
	v := interval.Seconds()
	s.observe(v, v, 1)
}

// ReportHistogramValueSamples implements tally.StatsReporter.
func (r *prometheusReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound, bucketUpperBound float64,
	samples int64,
) {
	r.reportHistogramSamples(
		name, tags, buckets.AsValues(), bucketLowerBound, bucketUpperBound, samples)
}

// ReportHistogramDurationSamples implements tally.StatsReporter. Durations
// are exported in seconds.
func (r *prometheusReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound, bucketUpperBound time.Duration,
	samples int64,
) {
	durations := buckets.AsDurations()
	bounds := make([]float64, len(durations))
	for i, d := range durations {
		bounds[i] = d.Seconds()
	}
	lower := durationBoundToSeconds(bucketLowerBound)
	upper := durationBoundToSeconds(bucketUpperBound)
	r.reportHistogramSamples(name, tags, bounds, lower, upper, samples)
}

func (r *prometheusReporter) reportHistogramSamples(
	name string,
	tags map[string]string,
	bounds []float64,
	lower, upper float64,
	samples int64,
) {
	if samples <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.getSeries(name, tags, _promHistogram)
	if s == nil {
		return
	}
	if s.counts == nil {
		s.initBuckets(bounds)
	}
	// tally only reports how many samples fell into a bucket, not their
	// values, so the sum is estimated from the bucket bounds.
	// Samples in the overflow bucket are estimated by its lower bound.
	estimate := upper
	if upper >= math.MaxFloat64 {
		estimate = lower
	}
	s.observe(upper, estimate, uint64(samples))
}

// initBuckets sets up histogram buckets from tally bucket bounds, which are
// sorted and deduplicated. Infinite bounds are dropped since Prometheus
// always adds the +Inf bucket.
func (s *promSeries) initBuckets(bounds []float64) {
	sorted := append([]float64(nil), bounds...)
	sort.Float64s(sorted)
	s.upperBounds = make([]float64, 0, len(sorted))
	for _, b := range sorted {
		if math.IsInf(b, 0) || b >= math.MaxFloat64 || b <= -math.MaxFloat64 {
			continue
		}
		if n := len(s.upperBounds); n > 0 && s.upperBounds[n-1] == b {
			continue
		}
		s.upperBounds = append(s.upperBounds, b)
	}
	s.counts = make([]uint64, len(s.upperBounds)+1)
}

// observe records n samples whose bucket is chosen by v (the first bucket
// whose upper bound is >= v) and adds sumEach*n to the sum.
func (s *promSeries) observe(v, sumEach float64, n uint64) {
	i := sort.SearchFloat64s(s.upperBounds, v)
	s.counts[i] += n
	s.count += n
	s.sum += sumEach * float64(n)
}

// Capabilities implements tally.StatsReporter.
func (r *prometheusReporter) Capabilities() tally.Capabilities { return r }

// Reporting implements tally.Capabilities.
func (r *prometheusReporter) Reporting() bool { return true }

// Tagging implements tally.Capabilities.
func (r *prometheusReporter) Tagging() bool { return true }

// Flush implements tally.StatsReporter. Metrics are pulled on scrape, so
// there is nothing to flush.
func (r *prometheusReporter) Flush() {}

func durationBoundToSeconds(d time.Duration) float64 {
	switch d {
	case time.Duration(math.MaxInt64):
		return math.MaxFloat64
	case time.Duration(math.MinInt64):
		return -math.MaxFloat64
	}
	return d.Seconds()
}
