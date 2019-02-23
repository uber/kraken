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
package store

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"

	"github.com/uber/kraken/utils/osutil"
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

// walkDirectory is a helper function which scans the given dir and perform
// specified functions at given depth.
// This function doesn't wrap errors.
//
// Note: This could be an expensive operation and will potentially return stale
// data.
func walkDirectory(rootDir string, depth int, f func(string) error) error {
	if depth == 0 {
		empty, err := osutil.IsEmpty(rootDir)
		if err != nil {
			return err
		}
		if !empty {
			if err = f(rootDir); err != nil {
				return err
			}
		}
	} else {
		infos, err := ioutil.ReadDir(rootDir)
		if err != nil {
			return err
		}
		for _, info := range infos {
			if info.IsDir() {
				if err := walkDirectory(path.Join(rootDir, info.Name()), depth-1, f); err != nil {
					return err
				}
			}
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
