package metrics

import (
	"fmt"
	"io"
	"log"

	"github.com/uber-go/tally"
)

func init() {
	register("statsd", newStatsdScope)
	register("default", newDefaultScope)
}

var _scopeFactories = make(map[string]scopeFactory)

type scopeFactory func(config Config) (tally.Scope, io.Closer, error)

func register(name string, f scopeFactory) {
	if _, ok := _scopeFactories[name]; ok {
		log.Fatalf("Scope factory %q is already registered", name)
	}
	_scopeFactories[name] = f
}

// New creates a new metrics Scope from config. If no backend is configured, a default
// scope is used.
func New(config Config) (tally.Scope, io.Closer, error) {
	if config.Backend == "" {
		config.Backend = "default"
	}
	f, ok := _scopeFactories[config.Backend]
	if !ok || f == nil {
		return nil, nil, fmt.Errorf("metrics backend %q not registered", config.Backend)
	}
	return f(config)
}
