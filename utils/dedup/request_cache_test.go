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
package dedup

import (
	"errors"
	"testing"
	"time"

	"github.com/uber/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func block() error {
	select {}
}

func noop() error {
	return nil
}

func TestRequestCacheStartPreventsDuplicates(t *testing.T) {
	d := NewRequestCache(RequestCacheConfig{}, clock.New())

	id := "foo"

	require.NoError(t, d.Start(id, block))
	require.Equal(t, ErrRequestPending, d.Start(id, block))
}

func TestRequestCacheStartClearsPendingWhenFuncDone(t *testing.T) {
	d := NewRequestCache(RequestCacheConfig{}, clock.New())

	id := "foo"

	require.NoError(t, d.Start(id, noop))
	require.NoError(t, testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == nil
	}))
}

func TestRequestCacheCachesErrors(t *testing.T) {
	clk := clock.NewMock()
	d := NewRequestCache(RequestCacheConfig{}, clk)

	id := "foo"
	err := errors.New("some error")

	require.NoError(t, d.Start(id, func() error { return err }))
	require.NoError(t, testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == err
	}))
}

func TestRequestCacheExpiresErrors(t *testing.T) {
	config := RequestCacheConfig{
		ErrorTTL: 5 * time.Second,
	}
	clk := clock.NewMock()
	d := NewRequestCache(config, clk)

	id := "foo"
	err := errors.New("some error")

	require.NoError(t, d.Start(id, func() error { return err }))
	require.NoError(t, testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == err
	}))

	clk.Add(config.ErrorTTL + 1)

	require.NoError(t, d.Start(id, noop))
}

func TestRequestCacheExpiresNotFoundErrorsIndependently(t *testing.T) {
	config := RequestCacheConfig{
		ErrorTTL:    5 * time.Second,
		NotFoundTTL: 30 * time.Second,
	}
	clk := clock.NewMock()
	d := NewRequestCache(config, clk)

	id := "foo"
	errNotFound := errors.New("error not found")
	d.SetNotFound(func(err error) bool { return err == errNotFound })

	require.NoError(t, d.Start(id, func() error { return errNotFound }))
	require.NoError(t, testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == errNotFound
	}))

	clk.Add(config.ErrorTTL + 1)

	require.Equal(t, errNotFound, d.Start(id, noop))

	clk.Add(config.NotFoundTTL + 1)

	require.NoError(t, d.Start(id, noop))
}

func TestRequestCacheStartCleansUpCachedErrors(t *testing.T) {
	config := RequestCacheConfig{
		ErrorTTL:        5 * time.Second,
		CleanupInterval: 10 * time.Second,
	}
	clk := clock.NewMock()
	d := NewRequestCache(config, clk)

	err := errors.New("some error")

	require.NoError(t, d.Start("a", func() error { return err }))
	require.NoError(t, d.Start("b", func() error { return err }))
	require.NoError(t, d.Start("c", noop))

	require.NoError(t, testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start("a", noop) == err
	}))
	require.NoError(t, testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start("b", noop) == err
	}))

	clk.Add(config.ErrorTTL)
	clk.Add(config.CleanupInterval)

	require.NoError(t, d.Start("c", noop))

	// Start should trigger cleanup.
	require.Empty(t, d.errors)
}

func TestRequestCacheLimitsNumberOfWorkers(t *testing.T) {
	config := RequestCacheConfig{
		NumWorkers:  1,
		BusyTimeout: 100 * time.Millisecond,
	}
	d := NewRequestCache(config, clock.New())

	exit := make(chan bool)

	require.NoError(t, d.Start("a", func() error {
		<-exit
		return nil
	}))
	require.Equal(t, ErrWorkersBusy, d.Start("b", noop))

	// After a's function exits, the worker should be released.
	exit <- true
	require.NoError(t, d.Start("b", noop))
}
