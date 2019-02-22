package rwutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/uber/kraken/utils/randutil"
	"github.com/stretchr/testify/require"
)

func TestPlainReader(t *testing.T) {
	require := require.New(t)

	data := randutil.Text(32)

	var result bytes.Buffer
	_, err := io.Copy(&result, PlainReader(data))
	require.NoError(err)
	require.Equal(data, result.Bytes())
}

func TestPlainWriter(t *testing.T) {
	require := require.New(t)

	data := randutil.Text(32)

	w := make(PlainWriter, len(data))
	_, err := io.Copy(w, bytes.NewReader(data))
	require.NoError(err)
	require.Equal(data, []byte(w))
}
