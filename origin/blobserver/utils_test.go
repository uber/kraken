package blobserver

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/utils/handler"
	"github.com/stretchr/testify/require"
)

func TestParseContentRangeHeaderBadRequests(t *testing.T) {
	tests := []struct {
		description string
		value       string
	}{
		{"empty value", ""},
		{"invalid format", "blah"},
		{"invalid start", "blah-5"},
		{"invalid end", "5-blah"},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			h := http.Header{}
			h.Add("Content-Range", test.value)
			start, end, err := parseContentRange(h)
			require.Error(err)
			require.Equal(http.StatusBadRequest, err.(*handler.Error).GetStatus())
			require.Equal(int64(0), start)
			require.Equal(int64(0), end)
		})
	}
}
