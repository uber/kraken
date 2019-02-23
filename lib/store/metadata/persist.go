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

import (
	"regexp"
	"strconv"
)

const _persistSuffix = "_persist"

func init() {
	Register(regexp.MustCompile(_persistSuffix), &persistFactory{})
}

type persistFactory struct{}

func (f persistFactory) Create(suffix string) Metadata {
	return &Persist{}
}

// Persist marks whether a blob should be persisted.
type Persist struct {
	Value bool
}

// NewPersist creates a new Persist, where true means the blob
// should be persisted, and false means the blob is safe to delete.
func NewPersist(v bool) *Persist {
	return &Persist{v}
}

// GetSuffix returns a static suffix.
func (m *Persist) GetSuffix() string {
	return _persistSuffix
}

// Movable is true.
func (m *Persist) Movable() bool {
	return true
}

// Serialize converts m to bytes.
func (m *Persist) Serialize() ([]byte, error) {
	return []byte(strconv.FormatBool(m.Value)), nil
}

// Deserialize loads b into m.
func (m *Persist) Deserialize(b []byte) error {
	v, err := strconv.ParseBool(string(b))
	if err != nil {
		return err
	}
	m.Value = v
	return nil
}
