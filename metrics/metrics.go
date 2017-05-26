package metrics

import (
	"fmt"
	"io"

	"github.com/uber-go/tally"
)

var _metricsFactories = make(map[string]Reporter)

// Reporter defines a metrics interface
type Reporter interface {
	create(parameters map[string]interface{}) (tally.Scope, io.Closer, error)
}

// Register registers a new metricsDriverFactory given name
func Register(name string, reporter Reporter) {
	if reporter == nil {
		panic("Must not provide nil MetricsReportorFactory")
	}
	_, registered := _metricsFactories[name]
	if registered {
		panic(fmt.Sprintf("MetricsReportorFactory %s is already registered", name))
	}

	_metricsFactories[name] = reporter
}

// NewMetrics returns a new metric scope
func NewMetrics(config map[string]interface{}) (tally.Scope, io.Closer, error) {
	if config == nil || len(config) == 0 {
		defaultMetrics, ok := _metricsFactories[defaultName]
		if !ok || defaultMetrics == nil {
			return nil, nil, fmt.Errorf("Error initializing default metrics")
		}
		defaultMetrics.create(config)
	}

	typeParam, ok := config["type"]
	if !ok || typeParam == nil {
		return nil, nil, fmt.Errorf("Error initializing metrics type %v", typeParam)
	}
	metricsType, ok := typeParam.(string)
	if !ok {
		return nil, nil, fmt.Errorf("Error initializing metrics type %v", metricsType)
	}

	metrics, ok := _metricsFactories[metricsType]
	if !ok || metrics == nil {
		return nil, nil, fmt.Errorf("Error initializing metrics type %v: not registered", typeParam)
	}

	return metrics.create(config)
}
