package rwutil

import (
	"bytes"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"

	"code.uber.internal/infra/kraken/utils/memsize"
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
