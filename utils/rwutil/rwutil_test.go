package rwutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlainReader(t *testing.T) {
	require := require.New(t)

	data := []byte("blah blah blah")

	var result bytes.Buffer
	_, err := io.Copy(&result, PlainReader(data))
	require.NoError(err)
	require.Equal(data, result.Bytes())
}
