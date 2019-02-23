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
	"encoding/binary"
	"fmt"
	"regexp"
	"time"
)

var _lastAccessTimeSuffix = "_last_access_time"

func init() {
	Register(regexp.MustCompile(_lastAccessTimeSuffix), &lastAccessTimeFactory{})
}

type lastAccessTimeFactory struct{}

func (f lastAccessTimeFactory) Create(suffix string) Metadata {
	return &LastAccessTime{}
}

// LastAccessTime tracks a file's last access time.
type LastAccessTime struct {
	Time time.Time
}

// NewLastAccessTime creates a LastAccessTime from t.
func NewLastAccessTime(t time.Time) *LastAccessTime {
	return &LastAccessTime{t}
}

// GetSuffix returns the metadata suffix.
func (t *LastAccessTime) GetSuffix() string {
	return _lastAccessTimeSuffix
}

// Movable is true.
func (t *LastAccessTime) Movable() bool {
	return true
}

// Serialize converts t to bytes.
func (t *LastAccessTime) Serialize() ([]byte, error) {
	b := make([]byte, 8)
	binary.PutVarint(b, t.Time.Unix())
	return b, nil
}

// Deserialize loads b into t.
func (t *LastAccessTime) Deserialize(b []byte) error {
	i, n := binary.Varint(b)
	if n <= 0 {
		return fmt.Errorf("unmarshal last access time: %s", b)
	}
	t.Time = time.Unix(int64(i), 0)
	return nil
}
