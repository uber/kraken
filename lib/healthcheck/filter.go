package healthcheck

import (
	"context"
	"fmt"
	"sync"

	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/utils/stringset"
)

// Filter filters out unhealthy hosts from a host list.
type Filter interface {
	Init() error
	Run() error
	GetHealthy() stringset.Set
}

type filter struct {
	config  FilterConfig
	checker Checker
	hosts   hostlist.List
	state   *state
}

// NewFilter creates a Filter which applies checker to hosts to determine healthy
// hosts. If hosts only resolves to a single host, it is always declared healthy.
func NewFilter(config FilterConfig, checker Checker, hosts hostlist.List) Filter {
	config.applyDefaults()
	return &filter{
		config:  config,
		checker: checker,
		hosts:   hosts,
		state:   newState(config),
	}
}

// Init initializes the filter by assuming all resolved hosts are healthy. This
// is necessary when an entire cluster is restarting -- we cannot wait for other
// hosts to become healthy before starting, because they might be waiting for us
// to become healthy before starting.
func (f *filter) Init() error {
	addrs, err := f.hosts.ResolveNonLocal()
	if err != nil {
		return fmt.Errorf("hostlist: %s", err)
	}
	f.state.override(addrs)
	return nil
}

// Run runs the filter and updates the healthy hosts.
func (f *filter) Run() error {
	addrs, err := f.hosts.ResolveNonLocal()
	if err != nil {
		return fmt.Errorf("hostlist: %s", err)
	}
	if len(addrs) == 1 {
		// If hosts resolves to a single address, always mark it as healthy.
		f.state.override(addrs)
		return nil
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

	return nil
}

// GetHealthy returns the latest healthy hosts.
func (f *filter) GetHealthy() stringset.Set {
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
