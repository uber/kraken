package metrics

import (
	"io"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"
)

const (
	flushInterval = 100 * time.Millisecond
	flushBytes    = 512
	sampleRate    = 1.0
)

func newStatsdScope(config Config, cluster string) (tally.Scope, io.Closer, error) {
	statter, err := statsd.NewBufferedClient(
		config.Statsd.HostPort, config.Statsd.Prefix, flushInterval, flushBytes)
	if err != nil {
		return nil, nil, err
	}
	r := tallystatsd.NewReporter(statter, tallystatsd.Options{
		SampleRate: sampleRate,
	})
	s, c := tally.NewRootScope(tally.ScopeOptions{
		Reporter: r,
	}, time.Second)
	return s, c, nil
}
