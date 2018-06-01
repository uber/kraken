package core

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	_testStr     = "test"
	_expectedHex = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
)

func TestNewDigester(t *testing.T) {
	require := require.New(t)

	d := NewDigester()

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
}

func TestFromBytes(t *testing.T) {
	require := require.New(t)

	d := NewDigester()
	d.FromBytes([]byte(_testStr))

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
	require.Equal(_expectedHex, hexDigest)
}

func TestFromReader(t *testing.T) {
	require := require.New(t)

	d := NewDigester()
	r := strings.NewReader(_testStr)
	d.FromReader(r)

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
	require.Equal(_expectedHex, hexDigest)
}

func TestTeeReader(t *testing.T) {
	require := require.New(t)

	d := NewDigester()

	r := bytes.NewBufferString(_testStr)
	w := &bytes.Buffer{}
	tr := d.Tee(r)

	_, err := io.Copy(w, tr)
	require.NoError(err)
	b, err := ioutil.ReadAll(w)
	require.NoError(err)
	require.Equal(_testStr, string(b))

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
	require.Equal(_expectedHex, hexDigest)
}
