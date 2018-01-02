package dedup

import (
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/utils/testutil"

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
	require := require.New(t)

	d := NewRequestCache(RequestCacheConfig{}, clock.New())

	id := "foo"

	require.NoError(d.Start(id, block))
	require.Error(ErrRequestPending, d.Start(id, block))
}

func TestRequestCacheStartClearsPendingWhenFuncDone(t *testing.T) {
	require := require.New(t)

	d := NewRequestCache(RequestCacheConfig{}, clock.New())

	id := "foo"

	require.NoError(d.Start(id, noop))
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == nil
	}))
}

func TestRequestCacheCachesErrors(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	d := NewRequestCache(RequestCacheConfig{}, clk)

	id := "foo"
	err := errors.New("some error")

	require.NoError(d.Start(id, func() error { return err }))
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == err
	}))
}

func TestRequestCacheExpiresErrors(t *testing.T) {
	require := require.New(t)

	config := RequestCacheConfig{
		ErrorTTL: 5 * time.Second,
	}
	clk := clock.NewMock()
	d := NewRequestCache(config, clk)

	id := "foo"
	err := errors.New("some error")

	require.NoError(d.Start(id, func() error { return err }))
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == err
	}))

	clk.Add(config.ErrorTTL + 1)

	require.NoError(d.Start(id, noop))
}

func TestRequestCacheExpiresNotFoundErrorsIndependently(t *testing.T) {
	require := require.New(t)

	config := RequestCacheConfig{
		ErrorTTL:    5 * time.Second,
		NotFoundTTL: 30 * time.Second,
	}
	clk := clock.NewMock()
	d := NewRequestCache(config, clk)

	id := "foo"

	require.NoError(d.Start(id, func() error { return ErrNotFound }))
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start(id, noop) == ErrNotFound
	}))

	clk.Add(config.ErrorTTL + 1)

	require.Error(ErrNotFound, d.Start(id, noop))

	clk.Add(config.NotFoundTTL + 1)

	require.NoError(d.Start(id, noop))
}

func TestRequestCacheStartCleansUpCachedErrors(t *testing.T) {
	require := require.New(t)

	config := RequestCacheConfig{
		ErrorTTL:        5 * time.Second,
		CleanupInterval: 10 * time.Second,
	}
	clk := clock.NewMock()
	d := NewRequestCache(config, clk)

	err := errors.New("some error")

	require.NoError(d.Start("a", func() error { return err }))
	require.NoError(d.Start("b", func() error { return err }))
	require.NoError(d.Start("c", noop))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start("a", noop) == err
	}))
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		return d.Start("b", noop) == err
	}))

	clk.Add(config.ErrorTTL)
	clk.Add(config.CleanupInterval)

	d.Start("c", noop)

	// Start should trigger cleanup.
	require.Empty(d.errors)
}

func TestRequestCacheLimitsNumberOfWorkers(t *testing.T) {
	require := require.New(t)

	config := RequestCacheConfig{
		NumWorkers:  1,
		BusyTimeout: 100 * time.Millisecond,
	}
	d := NewRequestCache(config, clock.New())

	exit := make(chan bool)

	require.NoError(d.Start("a", func() error {
		<-exit
		return nil
	}))
	require.Error(ErrWorkersBusy, d.Start("b", noop))

	// After a's function exits, the worker should be released.
	exit <- true
	require.NoError(d.Start("b", noop))
}
