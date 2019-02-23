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
package flagutil

import (
	"flag"
	"strconv"
)

// Ints allows specifying a slice of ints via flags, where multiple flags of the
// same name append to the slice.
type Ints []int

var _ flag.Value = (*Ints)(nil)

func (i *Ints) String() string {
	return "list of ints"
}

// Set appends v to i.
func (i *Ints) Set(v string) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	*i = append(*i, n)
	return nil
}
