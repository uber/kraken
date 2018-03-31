package metainfogen

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestPieceLengthConfig(t *testing.T) {
	require := require.New(t)

	plConfig, err := newPieceLengthConfig(map[datasize.ByteSize]datasize.ByteSize{
		0:               datasize.MB,
		2 * datasize.GB: 4 * datasize.MB,
		4 * datasize.GB: 8 * datasize.MB,
	})
	require.NoError(err)

	require.Equal(int64(datasize.MB), plConfig.get(int64(datasize.GB)))
	require.Equal(int64(4*datasize.MB), plConfig.get(int64(2*datasize.GB)))
	require.Equal(int64(4*datasize.MB), plConfig.get(int64(3*datasize.GB)))
	require.Equal(int64(8*datasize.MB), plConfig.get(int64(4*datasize.GB)))
	require.Equal(int64(8*datasize.MB), plConfig.get(int64(8*datasize.GB)))
}
