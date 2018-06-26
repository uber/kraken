package store

import (
	"bytes"
	"io/ioutil"
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestSimpleStoreCreateCacheFile(t *testing.T) {
	require := require.New(t)

	s, cleanup := SimpleStoreFixture()
	defer cleanup()

	tag := core.TagFixture()
	d := core.DigestFixture().String()

	require.NoError(s.CreateCacheFile(tag, bytes.NewBufferString(d)))

	f, err := s.GetCacheFileReader(tag)
	require.NoError(err)
	result, err := ioutil.ReadAll(f)
	require.NoError(err)
	require.Equal(d, string(result))
}
