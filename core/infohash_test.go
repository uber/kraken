package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewInfoHashFromHex(t *testing.T) {
	require := require.New(t)

	d, err := NewInfoHashFromHex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4")
	require.NoError(err)
	require.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4", d.Hex())
	require.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4", d.String())
}

func TestNewInfoHashFromHexErrors(t *testing.T) {
	tests := []struct {
		desc  string
		input string
	}{
		{"empty", ""},
		{"too long", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"invalid hex", "x3b0c44298fc1c149afbf4c8996fb92427ae41e4"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := NewInfoHashFromHex(test.input)
			require.Error(t, err)
		})
	}
}
