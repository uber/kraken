package metadata

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistMetadataSerialization(t *testing.T) {
	for _, v := range []bool{true, false} {
		t.Run(strconv.FormatBool(v), func(t *testing.T) {
			require := require.New(t)

			p := NewPersist(v)
			b, err := p.Serialize()
			require.NoError(err)

			var result Persist
			require.NoError(result.Deserialize(b))
			require.Equal(p.Value, result.Value)
		})
	}
}
