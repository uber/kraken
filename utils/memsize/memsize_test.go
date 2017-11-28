package memsize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormat(t *testing.T) {
	tests := []struct {
		b        uint64
		expected string
	}{
		{0, "0B"},
		{20 * B, "20.00B"},
		{256 * KB, "256.00KB"},
		{90 * MB, "90.00MB"},
		{2 * GB, "2.00GB"},
		{5 * TB, "5.00TB"},
		{GB + 512*MB, "1.50GB"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			require.Equal(t, test.expected, Format(test.b))
		})
	}
}
