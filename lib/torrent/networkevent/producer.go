package networkevent

import (
	"encoding/json"
	"errors"
	"fmt"

	"code.uber.internal/go-common.git/x/kafka"
	"code.uber.internal/go-common.git/x/log"
)

// Producer emits events.
type Producer interface {
	Produce(Event) error
}

type producer struct {
	config Config
	rest   *kafka.RestProducer
}

// NewProducer creates a new Kafka producer.
func NewProducer(config Config) (Producer, error) {
	rest, err := kafka.NewRestProducer()
	if err != nil {
		return nil, err
	}
	if config.Enabled && config.KafkaTopic == "" {
		return nil, errors.New("no kafka topic supplied")
	}
	if !config.Enabled {
		log.Warn("Kafka network events not enabled")
	}
	return &producer{config, rest}, nil
}

// Produce publishes e on the configured Kafka topic.
func (p *producer) Produce(e Event) error {
	if !p.config.Enabled {
		return nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("json marshal: %s", err)
	}
	return p.rest.Produce(p.config.KafkaTopic, b)
}
