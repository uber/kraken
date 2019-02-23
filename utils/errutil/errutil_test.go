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

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiError(t *testing.T) {
	a := errors.New("a")
	b := errors.New("b")
	c := errors.New("c")

	tests := []struct {
		description string
		errs        []error
		result      string
	}{
		{"empty", nil, ""},
		{"one error", []error{a}, "a"},
		{"many errors", []error{a, b, c}, "a, b, c"},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require.Equal(t, test.result, MultiError(test.errs).Error())
		})
	}
}

func TestJoinNil(t *testing.T) {
	f := func() error {
		var errs []error
		return Join(errs)
	}
	require.NoError(t, f())
}

func TestJoinNonNil(t *testing.T) {
	f := func() error {
		var errs []error
		errs = append(errs, errors.New("some error"))
		return Join(errs)
	}
	require.Error(t, f())
}
