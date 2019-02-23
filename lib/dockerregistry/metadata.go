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
package dockerregistry

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/uber/kraken/lib/store/metadata"
)

const (
	_startedAtSuffix = "_startedat"
	_startedAtLayout = time.RFC3339
)

func init() {
	metadata.Register(regexp.MustCompile(_startedAtSuffix), &startedAtMetadataFactory{})

	// TODO(evelynl): use _ instead of /, otherwise it won't support reload.
	metadata.Register(regexp.MustCompile("_hashstates/\\w+/\\w+$"), &hashStateMetadataFactory{})
}

type startedAtMetadataFactory struct{}

func (f startedAtMetadataFactory) Create(suffix string) metadata.Metadata {
	return &startedAtMetadata{}
}

// startedAtMetadata is used to remove uploads that have been active for too long.
type startedAtMetadata struct {
	time time.Time
}

func newStartedAtMetadata(t time.Time) *startedAtMetadata {
	return &startedAtMetadata{t}
}

func (s *startedAtMetadata) GetSuffix() string {
	return _startedAtSuffix
}

func (s *startedAtMetadata) Movable() bool {
	return false
}

func (s *startedAtMetadata) Serialize() ([]byte, error) {
	return []byte(s.time.Format(_startedAtLayout)), nil
}

func (s *startedAtMetadata) Deserialize(b []byte) error {
	t, err := time.Parse(_startedAtLayout, string(b))
	if err != nil {
		return err
	}
	s.time = t
	return nil
}

type hashStateMetadataFactory struct{}

func (f hashStateMetadataFactory) Create(suffix string) metadata.Metadata {
	parts := strings.Split(suffix, "/")
	if len(parts) != 3 {
		return nil
	}
	algo := parts[1]
	offset := parts[2]
	return newHashStateMetadata(algo, offset)
}

// hashStateMetadata stores partial hash result of upload data for resumable upload.
// Docker registry double writes to a writer and digester, and the digester
// generates this snapshot.
type hashStateMetadata struct {
	algo    string
	offset  string
	content []byte
}

func newHashStateMetadata(algo, offset string) *hashStateMetadata {
	return &hashStateMetadata{
		algo:   algo,
		offset: offset,
	}
}

func (h *hashStateMetadata) GetSuffix() string {
	return fmt.Sprintf("_hashstates/%s/%s", h.algo, h.offset)
}

func (h *hashStateMetadata) Movable() bool {
	return false
}

func (h *hashStateMetadata) Serialize() ([]byte, error) {
	return h.content, nil
}

func (h *hashStateMetadata) Deserialize(b []byte) error {
	h.content = b
	return nil
}

func (h *hashStateMetadata) dockerPath() string {
	return fmt.Sprintf("hashstates/%s/%s", h.algo, h.offset)
}
