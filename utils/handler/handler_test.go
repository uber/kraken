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
package handler_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
)

func TestIsStatus(t *testing.T) {
	tests := []struct {
		desc     string
		err      error
		status   int
		expected bool
	}{
		{"matching status", handler.ErrorStatus(http.StatusConflict), http.StatusConflict, true},
		{"different status", handler.ErrorStatus(http.StatusConflict), http.StatusNotFound, false},
		{"error with message", handler.Errorf("taken").Status(http.StatusConflict), http.StatusConflict, true},
		{"default status", handler.Errorf("boom"), http.StatusInternalServerError, true},
		{
			"wrapped error",
			fmt.Errorf("start upload: %w", handler.ErrorStatus(http.StatusConflict)),
			http.StatusConflict,
			true,
		},
		{"non-handler error", errors.New("conflict"), http.StatusConflict, false},
		{"nil error", nil, http.StatusConflict, false},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			require.Equal(t, tt.expected, handler.IsStatus(tt.err, tt.status))
		})
	}
}

func TestIsStatusMatchesWhatHTTPUtilCannot(t *testing.T) {
	err := handler.ErrorStatus(http.StatusConflict)

	require.False(t, httputil.IsConflict(err))
	require.True(t, handler.IsStatus(err, http.StatusConflict))
}
