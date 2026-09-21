// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package store

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/log"
)

func createOrUpdateSymlink(sourcePath, targetPath string) error {
	if _, err := os.Stat(targetPath); err == nil {
		if existingSource, err := os.Readlink(targetPath); err != nil {
			return err
		} else if existingSource != sourcePath {
			// If the symlink already exists and points to another valid location, recreate the symlink.
			if err := os.Remove(targetPath); err != nil {
				return err
			}
			if err := os.Symlink(sourcePath, targetPath); err != nil {
				return err
			}
		}
	} else if os.IsNotExist(err) {
		if err := os.Symlink(sourcePath, targetPath); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}

func verifyDigest(r io.Reader, name string, skipHashVerification bool) error {
	expected, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return fmt.Errorf("new digest from file name: %w", err)
	}

	if !skipHashVerification {
		computed, err := core.NewDigester().FromReader(r)
		if err != nil {
			return fmt.Errorf("calculate digest: %w", err)
		}
		if computed != expected {
			log.With("name", name, "expected", expected, "computed", computed).Error("Digest verification did not match")
			return fmt.Errorf("computed digest %s doesn't match expected value %s", computed, expected)
		}
	}
	return nil
}

type bufferFileReader struct {
	*bytes.Reader
}

func (b *bufferFileReader) Close() error {
	return nil
}

// NewBufferFileReader returns an in-memory FileReader backed by b.
func NewBufferFileReader(b []byte) FileReader {
	return &bufferFileReader{bytes.NewReader(b)}
}
