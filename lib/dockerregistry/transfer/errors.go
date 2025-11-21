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
package transfer

import "fmt"

// ErrBlobNotFound is returned when a blob is not found by transferer.
type ErrBlobNotFound struct {
	Digest string
	Reason string
}

func (e ErrBlobNotFound) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("blob %s not found: %s", e.Digest, e.Reason)
	}
	return fmt.Sprintf("blob %s not found", e.Digest)
}

// ErrTagNotFound is returned when a tag is not found by transferer.
type ErrTagNotFound struct {
	Tag    string
	Reason string
}

func (e ErrTagNotFound) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("tag %s not found: %s", e.Tag, e.Reason)
	}
	return fmt.Sprintf("tag %s not found", e.Tag)
}

// IsBlobNotFound checks if an error is ErrBlobNotFound.
func IsBlobNotFound(err error) bool {
	_, ok := err.(ErrBlobNotFound)
	if ok {
		return true
	}
	_, ok = err.(*ErrBlobNotFound)
	return ok
}

// IsTagNotFound checks if an error is ErrTagNotFound.
func IsTagNotFound(err error) bool {
	_, ok := err.(ErrTagNotFound)
	if ok {
		return true
	}
	_, ok = err.(*ErrTagNotFound)
	return ok
}
