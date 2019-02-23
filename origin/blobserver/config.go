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
package blobserver

import (
	"time"

	"github.com/uber/kraken/utils/listener"
)

// Config defines the configuration used by Origin cluster for hashing blob digests.
type Config struct {
	Listener                  listener.Config `yaml:"listener"`
	DuplicateWriteBackStagger time.Duration   `yaml:"duplicate_write_back_stagger"`
}

func (c Config) applyDefaults() Config {
	if c.DuplicateWriteBackStagger == 0 {
		c.DuplicateWriteBackStagger = 30 * time.Minute
	}
	return c
}
