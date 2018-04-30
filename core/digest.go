package core

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

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
func NewDigestFromString(str string) (Digest, error) {
	digest := Digest{}
	parts := strings.Split(str, ":")
	if len(parts) != 2 {
		return digest, fmt.Errorf("Digest %s is not correctly formatted", str)
	}

	digest.algo = parts[0]
	digest.hex = parts[1]
	digest.raw = str
	return digest, nil
}

// NewSHA256DigestFromHex creates a new Digest obj
func NewSHA256DigestFromHex(hexStr string) Digest {
	return Digest{
		algo: SHA256,
		hex:  hexStr,
		raw:  fmt.Sprintf("%s:%s", SHA256, hexStr),
	}
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

// ShardID returns the shard id of the digest.
func (d Digest) ShardID() string {
	return d.hex[:4]
}

// CheckSHA256Digest returns error if s is not a valid SHA256 hex digest.
func CheckSHA256Digest(s string) error {
	if len(s) != 64 {
		return errors.New("must be 64 characters")
	}
	if _, err := hex.DecodeString(s); err != nil {
		return fmt.Errorf("hex: %s", err)
	}
	return nil
}
