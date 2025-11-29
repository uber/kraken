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
package shutdown

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandler_Context(t *testing.T) {
	require := require.New(t)

	h := New(context.Background())
	require.NotNil(h.Context())

	// Context should not be cancelled initially
	select {
	case <-h.Context().Done():
		t.Fatal("context should not be cancelled")
	default:
	}

	// After shutdown, context should be cancelled
	h.Shutdown()

	select {
	case <-h.Context().Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context should be cancelled after shutdown")
	}
}

func TestHandler_AddCleanup(t *testing.T) {
	require := require.New(t)

	h := New(context.Background())

	var order []int
	h.AddCleanup(func() error {
		order = append(order, 1)
		return nil
	})
	h.AddCleanup(func() error {
		order = append(order, 2)
		return nil
	})
	h.AddCleanup(func() error {
		order = append(order, 3)
		return nil
	})

	h.Shutdown()

	// Cleanup functions should be called in LIFO order
	require.Equal([]int{3, 2, 1}, order)
}

func TestHandler_AddCleanup_WithError(t *testing.T) {
	require := require.New(t)

	h := New(context.Background())

	var called []int
	h.AddCleanup(func() error {
		called = append(called, 1)
		return nil
	})
	h.AddCleanup(func() error {
		called = append(called, 2)
		return errors.New("cleanup error")
	})
	h.AddCleanup(func() error {
		called = append(called, 3)
		return nil
	})

	h.Shutdown()

	// All cleanup functions should be called even if one fails
	require.Equal([]int{3, 2, 1}, called)
}

func TestHandler_Shutdown_OnlyOnce(t *testing.T) {
	require := require.New(t)

	h := New(context.Background())

	callCount := 0
	h.AddCleanup(func() error {
		callCount++
		return nil
	})

	// Call shutdown multiple times
	h.Shutdown()
	h.Shutdown()
	h.Shutdown()

	// Cleanup should only be called once
	require.Equal(1, callCount)
}
