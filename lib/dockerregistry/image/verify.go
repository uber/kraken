package image

import (
	"io"
)

// Verify hashes content of given digest
func Verify(digest Digest, reader io.Reader) (bool, error) {
	digester := NewDigester()
	computedDigest, err := digester.FromReader(reader)
	if err != nil {
		return false, err
	}

	return computedDigest == digest, nil
}
