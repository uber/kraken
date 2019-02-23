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
	_ "crypto/sha256" // For computing digest.
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// DigestEmptyTar is the sha256 digest of an empty tar file.
	DigestEmptyTar = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// DigestList is a list of digests.
type DigestList []Digest

// Value marshals a list of digests and returns []byte as driver.Value.
func (l DigestList) Value() (driver.Value, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return driver.Value([]byte{}), err
	}
	return driver.Value(b), nil
}

// Scan unmarshals []byte to a list of Digest.
func (l *DigestList) Scan(src interface{}) error {
	return json.Unmarshal(src.([]byte), l)
}

// Digest can be represented in a string like "<algorithm>:<hex_digest_string>"
// Example:
// 	 sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
type Digest struct {
	algo string
	hex  string
	raw  string
}

// NewSHA256DigestFromHex constructs a Digest from a sha256 in hexadecimal
// format. Returns error if hex is not a valid sha256.
func NewSHA256DigestFromHex(hex string) (Digest, error) {
	if err := ValidateSHA256(hex); err != nil {
		return Digest{}, fmt.Errorf("invalid sha256: %s", err)
	}
	return Digest{
		algo: SHA256,
		hex:  hex,
		raw:  fmt.Sprintf("%s:%s", SHA256, hex),
	}, nil
}

// ParseSHA256Digest parses a raw "<algo>:<hex>" sha256 digest. Returns error if the
// algo is not sha256 or the hex is not a valid sha256.
func ParseSHA256Digest(raw string) (Digest, error) {
	if raw == "" {
		return Digest{}, errors.New("invalid digest: empty")
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return Digest{}, errors.New("invalid digest: expected '<algo>:<hex>'")
	}
	algo := parts[0]
	hex := parts[1]
	if algo != SHA256 {
		return Digest{}, errors.New("invalid digest algo: expected sha256")
	}
	if err := ValidateSHA256(hex); err != nil {
		return Digest{}, fmt.Errorf("invalid sha256: %s", err)
	}
	return Digest{
		algo: algo,
		hex:  hex,
		raw:  raw,
	}, nil
}

// Value marshals a digest and returns []byte as driver.Value.
func (d Digest) Value() (driver.Value, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return driver.Value([]byte{}), err
	}
	return driver.Value(b), nil
}

// Scan unmarshals []byte to a Digest.
func (d *Digest) Scan(src interface{}) error {
	return json.Unmarshal(src.([]byte), d)
}

// UnmarshalJSON unmarshals "<algorithm>:<hex_digest_string>" to Digest.
func (d *Digest) UnmarshalJSON(str []byte) error {
	var raw string
	if err := json.Unmarshal(str, &raw); err != nil {
		return err
	}
	digest, err := ParseSHA256Digest(raw)
	if err != nil {
		return err
	}
	*d = digest
	return nil
}

// MarshalJSON unmarshals hexBytes to Digest.
func (d Digest) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.raw)
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

// ValidateSHA256 returns error if s is not a valid SHA256 hex digest.
func ValidateSHA256(s string) error {
	if len(s) != 64 {
		return fmt.Errorf("expected 64 characters, got %d from %q", len(s), s)
	}
	if _, err := hex.DecodeString(s); err != nil {
		return fmt.Errorf("hex: %s", err)
	}
	return nil
}
