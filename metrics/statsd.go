package metrics

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"
)

const (
	statsdName    = "statsd"
	flushInterval = 100 //millisec
	flushBytes    = 512
	sampleRate    = 1.0
)

func init() {
	Register(statsdName, &statsdReportor{})
}

// statsdReportor implements MetricsReporter
type statsdReportor struct {
	reportor tally.StatsReporter
}

// NewScope implements MetricsReporter.NewScope
func (r statsdReportor) create(parameters map[string]interface{}) (tally.Scope, io.Closer, error) {
	// Get parameters
	typeParam, ok := parameters["type"]
	if !ok || typeParam == nil {
		return nil, nil, errors.New("no type provided")
	}
	metricType, ok := typeParam.(string)
	if !ok || metricType != statsdName {
		return nil, nil, fmt.Errorf("type is not %s", statsdName)
	}

	hostportParam, ok := parameters["hostPort"]
	if !ok || hostportParam == nil {
		return nil, nil, errors.New("no hostPort provided")
	}
	metricHostPort, ok := hostportParam.(string)
	if !ok {
		return nil, nil, fmt.Errorf("invalid hostPort parameter: %v", hostportParam)
	}

	prefixParam, ok := parameters["prefix"]
	if !ok || prefixParam == nil {
		return nil, nil, errors.New("no prefix provided")
	}
	metricPrefix, ok := prefixParam.(string)
	if !ok {
		return nil, nil, fmt.Errorf("invalid prefix parameter: %v", prefixParam)
	}

	// Create statter, reporter and scope
	statter, _ := statsd.NewBufferedClient(metricHostPort, metricPrefix, flushInterval*time.Millisecond, flushBytes)
	r.reportor = tallystatsd.NewReporter(statter, tallystatsd.Options{
		SampleRate: sampleRate,
	})

	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Tags:     map[string]string{},
		Reporter: r.reportor,
	}, time.Second)

	return scope, closer, nil
}
