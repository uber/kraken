package metrics

import (
	"io"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
)

func newM3Scope(config Config) (tally.Scope, io.Closer, error) {
	// We have to do this ugly manual mapping because m3 configuration
	// uses validate:"nonzero" in the struct tags, meaning we would be
	// forced to specifcy m3 configuration even if we didn't use it.
	m3Config := m3.Configuration{
		HostPort: config.M3.HostPort,
		Service:  config.M3.Service,
		Env:      config.M3.Env,
	}
	r, err := m3Config.NewReporter()
	if err != nil {
		return nil, nil, err
	}
	s, c := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter: r,
	}, time.Second)
	return s, c, nil
}
