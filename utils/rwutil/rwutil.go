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
package rwutil

import "io"

// PlainReader provides an io.Reader for a bytes slice. It intentionally does
// not provide any other methods.
type PlainReader []byte

// Read always reads the entire underlying byte slice.
func (p PlainReader) Read(b []byte) (n int, err error) {
	copy(b, p)
	return len(p), io.EOF
}

// PlainWriter provides an io.Writer for a bytes slice. It intentionally does
// not provide any other methods. Clients should initialize length with make.
type PlainWriter []byte

// Write writes all of b to p.
func (p PlainWriter) Write(b []byte) (n int, err error) {
	copy(p, b)
	return len(p), nil
}
