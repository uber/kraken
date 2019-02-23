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
package metadata

import "regexp"

// Metadata defines types of matadata file.
// All implementations of Metadata must register themselves.
type Metadata interface {
	GetSuffix() string
	Movable() bool
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

var _factories = make(map[*regexp.Regexp]Factory)

// Factory creates Metadata objects given suffix.
type Factory interface {
	Create(suffix string) Metadata
}

// Register registers new Factory with corresponding suffix regexp.
func Register(suffix *regexp.Regexp, factory Factory) {
	_factories[suffix] = factory
}

// CreateFromSuffix creates a Metadata obj based on suffix.
// This is not a very efficient method; It's mostly used during reload.
func CreateFromSuffix(suffix string) Metadata {
	for re, factory := range _factories {
		if re.MatchString(suffix) {
			return factory.Create(suffix)
		}
	}
	return nil
}
