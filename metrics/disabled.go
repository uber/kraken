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
