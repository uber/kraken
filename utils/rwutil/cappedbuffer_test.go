package rwutil

import (
	"testing"

	"bytes"

	"github.com/stretchr/testify/require"
)

func TestCappedBuffer_write_drain_success(t *testing.T) {
	require := require.New(t)

	content := []byte("hello this is a stream of bytes")
	buffer := NewCappedBuffer(len(content))
	buffer.WriteAt(content[7:], 7)
	buffer.WriteAt(content[:7], 0)

	var dst bytes.Buffer
	buffer.DrainInto(&dst)
	require.Equal(content, dst.Bytes())
}

func TestCappedBuffer_write_drain_error(t *testing.T) {
	require := require.New(t)

	content := []byte("hello this is a stream of bytes")
	buffer := NewCappedBuffer(len(content) - 10)
	_, err := buffer.WriteAt(content[7:], 7)
	require.Error(err)

	_, err = buffer.WriteAt(content[:7], 0)
	require.NoError(err)
}
