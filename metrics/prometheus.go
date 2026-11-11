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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	_defaultPrometheusHandlerPath = "/metrics"
	_defaultPrometheusPrefix      = "kraken"
	_prometheusReportInterval     = time.Second
	_prometheusShutdownTimeout    = 5 * time.Second
)

// _defaultPrometheusTimerBuckets are the histogram buckets, in seconds, used
// for tally timers. They span 1ms to ~65s, which covers Kraken's request,
// download and drain latencies.
var _defaultPrometheusTimerBuckets = prometheus.ExponentialBuckets(0.001, 2, 17)

// PrometheusConfig configures the "prometheus" metrics backend, which serves
// metrics in the Prometheus exposition format for scraping.
type PrometheusConfig struct {
	// ListenAddress is the address the metrics HTTP server listens on, e.g.
	// ":9090". Required, since several Kraken components may share a host.
	ListenAddress string `yaml:"listen_address"`

	// HandlerPath is the HTTP path metrics are served on. Defaults to
	// "/metrics".
	HandlerPath string `yaml:"handler_path"`

	// Prefix is prepended to every metric name. Defaults to "kraken".
	Prefix string `yaml:"prefix"`

	// TimerBuckets are the histogram bucket upper bounds, in seconds, used for
	// timers. Defaults to exponential buckets from 1ms to ~65s.
	TimerBuckets []float64 `yaml:"timer_buckets"`
}

func (c PrometheusConfig) applyDefaults() PrometheusConfig {
	if c.HandlerPath == "" {
		c.HandlerPath = _defaultPrometheusHandlerPath
	}
	if c.Prefix == "" {
		c.Prefix = _defaultPrometheusPrefix
	}
	if len(c.TimerBuckets) == 0 {
		c.TimerBuckets = _defaultPrometheusTimerBuckets
	}
	return c
}

func (c PrometheusConfig) validate() error {
	if c.ListenAddress == "" {
		return errors.New("listen_address required for prometheus")
	}
	if !strings.HasPrefix(c.HandlerPath, "/") {
		return fmt.Errorf("handler_path %q must start with /", c.HandlerPath)
	}
	if !sort.Float64sAreSorted(c.TimerBuckets) {
		return errors.New("timer_buckets must be sorted in increasing order")
	}
	return nil
}
