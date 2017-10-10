package blobserver

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPushBlob(t *testing.T) {
	tests := []struct {
		description string
		blobSize    int64
		chunkSize   int64
	}{
		{"multiple chunks", 1024, 16},
		{"blob size smaller than chunk size", 15, 16},
		{"exactly one chunk", 16, 16},
		{"slightly larger blob size than chunk size", 17, 16},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			cp := newTestClientProvider(ClientConfig{UploadChunkSize: test.chunkSize})

			s := newTestServer(master1, configNoRedirectFixture(), cp)
			defer s.cleanup()

			d, blob := blobFixture(test.blobSize)

			err := cp.Provide(master1).PushBlob(d, bytes.NewBuffer(blob), test.blobSize)
			require.NoError(t, err)

			ensureHasBlob(t, s.fs, d, blob)
		})
	}
}
