package blobserver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigGetAddr(t *testing.T) {
	tests := []struct {
		description string
		hostname    string
		hashNodeMap HashNodeMap
		err         error
		addr        string
	}{
		{
			"master not in config",
			"master4",
			HashNodeMap{"master1:1": {}, "master2:2": {}, "master3:3": {}},
			portLookupError{"master4", 0},
			"",
		}, {
			"duplicated master in config",
			"master1",
			HashNodeMap{"master1:1": {}, "master2:2": {}, "master3:3": {}, "master1:4": {}},
			portLookupError{"master1", 2},
			"",
		}, {
			"found port in config",
			"master1",
			HashNodeMap{"master1:1": {}, "master2:2": {}, "master3:3": {}},
			nil,
			"master1:1",
		}, {
			"address does not contain port",
			"master1",
			HashNodeMap{"master1": {}},
			nil,
			"master1",
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)
			config := Config{HashNodes: test.hashNodeMap}
			addr, err := config.GetAddr(test.hostname)
			if test.err != nil {
				require.Equal(test.err, err)
			} else {
				require.NoError(err)
				require.Equal(test.addr, addr)
			}
		})
	}
}
