package netutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitHostPort(t *testing.T) {
	testCases := []struct {
		inputAddr string
		host      string
		port      string
		err       error
	}{
		{"master.com", "master.com", "", nil},
		{"master.com:1", "master.com", "1", nil},
		{":", "", "", fmt.Errorf(": is not a valid address")},
		{"master.com:1:1", "", "", fmt.Errorf("master.com:1:1 is not a valid address")},
		{"master.com:", "", "", fmt.Errorf("master.com: is not a valid address")},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("SpliteHostPort %s", test.inputAddr), func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			host, port, err := SplitHostPort(test.inputAddr)
			if test.err == nil {
				require.NoError(err)
				require.Equal(test.host, host)
				require.Equal(test.port, port)
				return
			}

			require.Error(err)
			require.Equal(test.err, err)
		})
	}
}
