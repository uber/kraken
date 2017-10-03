package image

import "strings"
import "fmt"

const (
	// DigestEmptyTar is the sha256 digest of an empty tar file.
	DigestEmptyTar = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// Digest can be represented in a string like "<algorithm>:<hex_digest_string>"
// Example:
// 	 sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
type Digest struct {
	algo string
	hex  string
	raw  string
}

// NewDigestFromString initializes a new Digest obj from given string.
func NewDigestFromString(str string) (*Digest, error) {
	parts := strings.Split(str, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Digest %s is not correctly formatted", str)
	}
	return &Digest{
		algo: parts[0],
		hex:  parts[1],
		raw:  str,
	}, nil
}

// String returns digest in string format like "<algorithm>:<hex_digest_string>".
func (d Digest) String() string {
	return d.raw
}

// Algo returns the algo part of the digest.
// Example:
//   sha256
func (d Digest) Algo() string {
	return d.algo
}

// Hex returns the hex part of the digest.
// Example:
//   e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
func (d Digest) Hex() string {
	return d.hex
}

// GetShardID returns the shard id of the digest.
func (d Digest) GetShardID() string {
	return d.hex[:4]
}
