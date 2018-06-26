package networkevent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/utils/log"
)

// Producer emits events.
type Producer interface {
	Produce(e *Event)
	Close() error
}

type producer struct {
	file *os.File
}

// NewProducer creates a new Producer.
func NewProducer(config Config) (Producer, error) {
	var f *os.File
	if config.Enabled {
		if config.LogPath == "" {
			return nil, errors.New("no log path supplied")
		}
		var flag int
		if _, err := os.Stat(config.LogPath); err != nil {
			if os.IsNotExist(err) {
				flag = os.O_WRONLY | os.O_CREATE | os.O_EXCL
			} else {
				return nil, fmt.Errorf("stat: %s", err)
			}
		} else {
			flag = os.O_WRONLY | os.O_APPEND
		}
		var err error
		f, err = os.OpenFile(config.LogPath, flag, 0775)
		if err != nil {
			return nil, fmt.Errorf("open %d: %s", flag, err)
		}
	} else {
		log.Warn("Kafka network events disabled")
	}
	return &producer{f}, nil
}

// Produce emits a network event.
func (p *producer) Produce(e *Event) {
	if p.file == nil {
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		log.Errorf("Error serializing network event to json: %s", err)
		return
	}
	line := append(b, byte('\n'))
	if _, err := p.file.Write(line); err != nil {
		log.Errorf("Error writing network event: %s", err)
		return
	}
}

// Close closes the producer.
func (p *producer) Close() error {
	if p.file == nil {
		return nil
	}
	return p.file.Close()
}
