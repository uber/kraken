package metrics

import (
	"io"
	"time"

	"github.com/uber-go/tally"
)

func newDefaultScope(Config) (tally.Scope, io.Closer, error) {
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:   "",
		Tags:     map[string]string{},
		Reporter: defaultReporter{},
	}, time.Second)
	return scope, closer, nil
}

// defaultReporter implements MetricsReporter
type defaultReporter struct{}

// ReportCounter implements tally.StatsReporter.ReportCounter
func (r defaultReporter) ReportCounter(name string, _ map[string]string, value int64) {}

// ReportGauge implements tally.StatsReporter.ReportGauge
func (r defaultReporter) ReportGauge(name string, _ map[string]string, value float64) {}

// ReportTimer implements tally.StatsReporter.ReportTimer
func (r defaultReporter) ReportTimer(name string, _ map[string]string, interval time.Duration) {}

// ReportHistogramValueSamples implements tally.StatsReporter.ReportHistogramValueSamples
func (r defaultReporter) ReportHistogramValueSamples(
	name string,
	_ map[string]string,
	_ tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64,
) {
}

// ReportHistogramDurationSamples implements tally.StatsReporter.ReportHistogramDurationSamples
func (r defaultReporter) ReportHistogramDurationSamples(
	name string,
	_ map[string]string,
	_ tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64,
) {
}

// Capabilities implements tally.StatsReporter.Capabilities
func (r defaultReporter) Capabilities() tally.Capabilities {
	return r
}

// Reporting implements tally.StatsReporter.Reporting
func (r defaultReporter) Reporting() bool {
	return true
}

// Tagging implements tally.StatsReporter.Tagging
func (r defaultReporter) Tagging() bool {
	return false
}

// Flush implements tally.StatsReporter.Flush
func (r defaultReporter) Flush() {}
