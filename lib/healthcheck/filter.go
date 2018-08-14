package healthcheck

import (
	"context"
	"sync"

	"code.uber.internal/infra/kraken/utils/stringset"
)

// Filter filters out unhealthy hosts from a host list.
type Filter interface {
	Run(addrs stringset.Set) stringset.Set
}

type filter struct {
	config  Config
	checker Checker
	state   *state
}

// NewFilter creates a new Filter. Filter is stateful -- consecutive runs are required
// to detect healthy / unhealthy hosts.
func NewFilter(config Config, checker Checker) Filter {
	config.applyDefaults()
	return &filter{
		config:  config,
		checker: checker,
		state:   newState(config),
	}
}

// Run applies checker to addrs against the current filter state and returns the
// healthy entries. New entries in addrs not found in the current state are
// assumed as initially healthy. If addrs only contains a single entry, it is
// always considered healthy.
func (f *filter) Run(addrs stringset.Set) stringset.Set {
	if len(addrs) == 1 {
		return addrs.Copy()
	}

	f.state.sync(addrs)

	ctx, cancel := context.WithTimeout(context.Background(), f.config.Timeout)
	defer cancel()

	var wg sync.WaitGroup
	for addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := f.check(ctx, addr); err != nil {
				f.state.failed(addr)
			} else {
				f.state.passed(addr)
			}
		}(addr)
	}
	wg.Wait()

	return f.state.getHealthy()
}

func (f *filter) check(ctx context.Context, addr string) error {
	errc := make(chan error, 1)
	go func() { errc <- f.checker.Check(ctx, addr) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errc:
		return err
	}
}
