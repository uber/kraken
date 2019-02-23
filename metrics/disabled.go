// Copyright (c) 2016-2019 Uber Technologies, Inc.
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
package metrics

import (
	"io"
	"time"

	"github.com/uber-go/tally"
)

func newDisabledScope(Config, string) (tally.Scope, io.Closer, error) {
	s, c := tally.NewRootScope(tally.ScopeOptions{
		Reporter: disabledReporter{},
	}, time.Second)
	return s, c, nil
}

type disabledReporter struct{}

func (r disabledReporter) ReportCounter(string, map[string]string, int64)       {}
func (r disabledReporter) ReportGauge(string, map[string]string, float64)       {}
func (r disabledReporter) ReportTimer(string, map[string]string, time.Duration) {}
func (r disabledReporter) ReportHistogramValueSamples(
	string, map[string]string, tally.Buckets, float64, float64, int64) {
}
func (r disabledReporter) ReportHistogramDurationSamples(
	string, map[string]string, tally.Buckets, time.Duration, time.Duration, int64) {
}
func (r disabledReporter) Capabilities() tally.Capabilities { return r }
func (r disabledReporter) Reporting() bool                  { return true }
func (r disabledReporter) Tagging() bool                    { return false }
func (r disabledReporter) Flush()                           {}
