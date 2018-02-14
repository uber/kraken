package core

import (
	"crypto"
	"fmt"
	"hash"
	"io"
)

const (
	// SHA256 is the only algorithm supported.
	SHA256 = "sha256"
)

// Digester calculates the digest of data stream.
type Digester struct {
	hash hash.Hash
}

// NewDigester instantiates and returns a new Digester object.
func NewDigester() *Digester {
	return &Digester{
		hash: crypto.SHA256.New(),
	}
}

// Digest returns the digest of existing data.
func (d *Digester) Digest() Digest {
	// Safe to ignore error.
	digest, _ := NewDigestFromString(fmt.Sprintf("%s:%x", SHA256, d.hash.Sum(nil)))
	return digest
}

// FromReader returns the digest of data from reader.
func (d Digester) FromReader(rd io.Reader) (Digest, error) {
	if _, err := io.Copy(d.hash, rd); err != nil {
		return Digest{}, err
	}

	return d.Digest(), nil
}

// FromBytes digests the input and returns a Digest.
func (d Digester) FromBytes(p []byte) (Digest, error) {
	if _, err := d.hash.Write(p); err != nil {
		return Digest{}, err
	}

	return d.Digest(), nil
}

// Tee allows d to calculate a digest of r while the caller reads from the
// returned reader.
func (d Digester) Tee(r io.Reader) io.Reader {
	return io.TeeReader(r, d.hash)
}
