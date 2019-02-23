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
package base

import (
	"regexp"
	"strings"

	"github.com/uber/kraken/lib/store/metadata"
)

// Mock metadata
func init() {
	metadata.Register(regexp.MustCompile("_mocksuffix_\\w+"), &mockMetadataFactory{})
	metadata.Register(regexp.MustCompile("_mockmovable"), &mockMetadataFactoryMovable{})
}

type mockMetadataFactory struct{}

func (f mockMetadataFactory) Create(suffix string) metadata.Metadata {
	if strings.HasSuffix(suffix, getMockMetadataOne().GetSuffix()) {
		return getMockMetadataOne()
	}
	if strings.HasSuffix(suffix, getMockMetadataTwo().GetSuffix()) {
		return getMockMetadataTwo()
	}
	return nil
}

type mockMetadata struct {
	randomSuffix string
	content      []byte
}

func getMockMetadataOne() *mockMetadata {
	return &mockMetadata{
		randomSuffix: "_mocksuffix_one",
	}
}

func getMockMetadataTwo() *mockMetadata {
	return &mockMetadata{
		randomSuffix: "_mocksuffix_two",
	}
}

func (m *mockMetadata) GetSuffix() string {
	return m.randomSuffix
}

func (m *mockMetadata) Movable() bool {
	return false
}

func (m *mockMetadata) Serialize() ([]byte, error) {
	return m.content, nil
}

func (m *mockMetadata) Deserialize(b []byte) error {
	m.content = b
	return nil
}

type mockMetadataFactoryMovable struct{}

func (f mockMetadataFactoryMovable) Create(suffix string) metadata.Metadata {
	return getMockMetadataMovable()
}

type mockMetadataMovable struct {
	randomSuffix string
	content      []byte
}

func getMockMetadataMovable() *mockMetadataMovable {
	return &mockMetadataMovable{
		randomSuffix: "_mockmovable",
	}
}

func (m *mockMetadataMovable) GetSuffix() string {
	return m.randomSuffix
}

func (m *mockMetadataMovable) Movable() bool {
	return true
}

func (m *mockMetadataMovable) Serialize() ([]byte, error) {
	return m.content, nil
}

func (m *mockMetadataMovable) Deserialize(b []byte) error {
	m.content = b
	return nil
}
