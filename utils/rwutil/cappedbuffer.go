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

import (
	"bytes"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/uber/kraken/utils/memsize"
)

type exceededCapError error

// CappedBuffer is a buffer that returns errors if the buffer exceeds cap.
type CappedBuffer struct {
	capacity int64
	buffer   *aws.WriteAtBuffer
}

// NewCappedBuffer creates a new CappedBuffer with the given capacity
func NewCappedBuffer(capacity int) *CappedBuffer {
	return &CappedBuffer{capacity: int64(capacity), buffer: aws.NewWriteAtBuffer([]byte{})}
}

// WriteAt writes the slice of bytes into CappedBuffer at given position
func (b *CappedBuffer) WriteAt(p []byte, pos int64) (n int, err error) {
	if pos+int64(len(p)) > b.capacity {
		return 0, exceededCapError(
			fmt.Errorf("buffer exceed max capacity %s", memsize.Format(uint64(b.capacity))))
	}
	return b.buffer.WriteAt(p, pos)
}

// DrainInto copies/drains/empties contents of CappedBuffer into dst
func (b *CappedBuffer) DrainInto(dst io.Writer) error {
	if _, err := io.Copy(dst, bytes.NewReader(b.buffer.Bytes())); err != nil {
		return fmt.Errorf("drain buffer: %s", err)
	}
	return nil
}
