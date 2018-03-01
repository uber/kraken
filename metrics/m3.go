package metrics

import (
	"fmt"
	"io"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
)

func newM3Scope(config Config, cluster string) (tally.Scope, io.Closer, error) {
	if cluster == "" {
		return nil, nil, fmt.Errorf("cluster required for m3")
	}
	m3Config := m3.Configuration{
		HostPort: config.M3.HostPort,
		Service:  config.M3.Service,
		Env:      cluster,
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
