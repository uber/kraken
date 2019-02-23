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
package mockutil

import (
	"bytes"
	"io"
	"io/ioutil"
	"regexp"
)

// RegexMatcher is a gomock Matcher which matches strings against some
// given regex.
type RegexMatcher struct {
	expected *regexp.Regexp
}

// MatchRegex returns a new RegexMatcher which matches the expected regex.
func MatchRegex(expected string) *RegexMatcher {
	return &RegexMatcher{regexp.MustCompile(expected)}
}

// Matches returns true if x is a string which matches the expected regex.
func (m *RegexMatcher) Matches(x interface{}) bool {
	s, ok := x.(string)
	if !ok {
		return false
	}
	return m.expected.MatchString(s)
}

func (m *RegexMatcher) String() string {
	return m.expected.String()
}

// ReaderMatcher is a gomock Matcher which matches io.Readers which produce some
// given bytes.
type ReaderMatcher struct {
	expected []byte
}

// MatchReader returns a new ReaderMatcher which matches expected.
func MatchReader(expected []byte) *ReaderMatcher {
	return &ReaderMatcher{expected}
}

// Matches returns true if x is an io.Reader which contains the expected bytes.
func (m *ReaderMatcher) Matches(x interface{}) bool {
	r, ok := x.(io.Reader)
	if !ok {
		return false
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return bytes.Compare(m.expected, b) == 0
}

func (m *ReaderMatcher) String() string {
	return string(m.expected)
}

// WriterMatcher is a gomock Matcher which matches any io.Writer, with the
// side-effect of writing some given bytes.
type WriterMatcher struct {
	b []byte
}

// MatchWriter returns a new WriterMatcher which write b to any io.Writer passed
// to Matches.
func MatchWriter(b []byte) *WriterMatcher {
	return &WriterMatcher{b}
}

// Matches writes given bytes to x.
func (m *WriterMatcher) Matches(x interface{}) bool {
	w, ok := x.(io.Writer)
	if !ok {
		return false
	}
	if _, err := w.Write(m.b); err != nil {
		panic(err)
	}
	return true
}

func (m *WriterMatcher) String() string {
	return "WriterMatcher"
}

// WriterAtMatcher is a gomock Matcher which matches any io.WriterAt, with the
// side-effect of writing some give bytes.
type WriterAtMatcher struct {
	b []byte
}

// MatchWriterAt returns a new WriterAtMatcher which writes b to any io.WriterAt passed
// to Matches.
func MatchWriterAt(b []byte) *WriterAtMatcher {
	return &WriterAtMatcher{b}
}

// Matches writes given bytes to x.
func (m *WriterAtMatcher) Matches(x interface{}) bool {
	w, ok := x.(io.WriterAt)
	if !ok {
		return false
	}
	if _, err := w.WriteAt(m.b, 0); err != nil {
		panic(err)
	}
	return true
}

func (m *WriterAtMatcher) String() string {
	return "WriterAtMatcher"
}
