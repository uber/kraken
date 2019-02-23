// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package networkevent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/uber/kraken/utils/log"
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
