package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckSHA256Digest(t *testing.T) {
	tests := []struct {
		desc  string
		input string
		err   bool
	}{
		{"valid", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", false},
		{"too short", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85", true},
		{"too long", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8556", true},
		{"invalid hex", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", true},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			err := CheckSHA256Digest(test.input)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
