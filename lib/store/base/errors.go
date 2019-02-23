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
package base

import "fmt"

// FileStateError represents errors related to file state.
// It's used when a file is not in the state it was supposed to be in.
type FileStateError struct {
	Op    string
	Name  string
	State FileState
	Msg   string
}

func (e *FileStateError) Error() string {
	return fmt.Sprintf("failed to perform \"%s\" on %s/%s: %s",
		e.Op, e.State.GetDirectory(), e.Name, e.Msg)
}

// IsFileStateError returns true if the param is of FileStateError type.
func IsFileStateError(err error) bool {
	_, ok := err.(*FileStateError)
	return ok
}
