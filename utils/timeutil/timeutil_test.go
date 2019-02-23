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
package timeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMostRecent(t *testing.T) {
	a := time.Now()
	b := a.Add(time.Second)
	c := b.Add(time.Second)

	for _, test := range []struct {
		description string
		ts          []time.Time
		expected    time.Time
	}{
		{"empty list", []time.Time{}, time.Time{}},
		{"single item", []time.Time{a}, a},
		{"ascending order", []time.Time{a, b, c}, c},
		{"descending order", []time.Time{c, b, a}, c},
	} {
		t.Run(test.description, func(t *testing.T) {
			actual := MostRecent(test.ts...)
			require.Equal(t, test.expected, actual)
		})
	}
}
