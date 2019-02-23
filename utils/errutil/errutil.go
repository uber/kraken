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
package errutil

import "bytes"

// MultiError defines a list of multiple errors. Useful for type assertions and
// inspecting individual errors.
//
// XXX: Don't initialize errors as MultiError unless you know what you're doing!
// See https://golang.org/doc/faq#nil_error for more details.
type MultiError []error

func (e MultiError) Error() string {
	var b bytes.Buffer
	for i, err := range e {
		b.WriteString(err.Error())
		if i < len(e)-1 {
			b.WriteString(", ")
		}
	}
	return b.String()
}

// Join converts errs into an error interface.
func Join(errs []error) error {
	if errs == nil {
		return nil
	}
	return MultiError(errs)
}
