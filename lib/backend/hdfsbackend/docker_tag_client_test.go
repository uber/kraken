package hdfsbackend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDockerTagClientPathErrors(t *testing.T) {
	for _, name := range []string{
		"4dfa0d38b99b774aabfde9a62421ac787ab168369e92421df968c7348893b60c",
		":",
		"repo:",
		":tag",
	} {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			client, err := NewDockerTagClient(configFixture("dummy-addr"))
			require.NoError(err)

			_, err = client.path(name)
			require.Error(err)
		})
	}
}
