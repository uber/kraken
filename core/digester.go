// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core

import (
	"crypto"
	"encoding/hex"
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
	digest, err := NewSHA256DigestFromHex(hex.EncodeToString(d.hash.Sum(nil)))
	if err != nil {
		// This should never fail.
		panic(err)
	}
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
