package dedup_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/mocks/utils/dedup"
	. "code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestCacheGetConcurrency(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resolver := mockdedup.NewMockResolver(ctrl)

	cache := NewCache(CacheConfig{}, clock.New(), resolver)

	kvs := make(map[string]string)
	for i := 0; i < 100; i++ {
		kvs[string(randutil.Text(32))] = string(randutil.Text(32))
	}

	for k, v := range kvs {
		resolver.EXPECT().Resolve(nil, k).Return(v, nil).MaxTimes(1)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Select random key.
			var k string
			for k = range kvs {
				break
			}
			v, err := cache.Get(nil, k)
			require.NoError(err)
			require.Equal(kvs[k], v.(string))
		}()
	}
	wg.Wait()
}

func TestCacheGetError(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resolver := mockdedup.NewMockResolver(ctrl)

	cache := NewCache(CacheConfig{}, clock.New(), resolver)

	k := "some key"
	err := errors.New("some error")

	resolver.EXPECT().Resolve(nil, k).Return("", err)

	for i := 0; i < 10; i++ {
		v, e := cache.Get(nil, k)
		require.Equal(err, e)
		require.Equal("", v.(string))
	}
}

func TestCacheGetCleanup(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resolver := mockdedup.NewMockResolver(ctrl)

	config := CacheConfig{
		ErrorTTL:        time.Minute,
		TTL:             time.Hour,
		CleanupInterval: 10 * time.Second,
	}

	clk := clock.NewMock()
	clk.Set(time.Now())

	cache := NewCache(config, clk, resolver)

	kvs := make(map[string]string)
	for i := 0; i < 1; i++ {
		kvs[string(randutil.Text(32))] = string(randutil.Text(32))
	}
	kerrs := make(map[string]error)
	for i := 0; i < 1; i++ {
		kerrs[string(randutil.Text(32))] = errors.New(string(randutil.Text(32)))
	}

	expectVals := func() {
		for k, v := range kvs {
			resolver.EXPECT().Resolve(nil, k).Return(v, nil)
		}
	}

	expectErrs := func() {
		for k, err := range kerrs {
			resolver.EXPECT().Resolve(nil, k).Return("", err)
		}
	}

	get := func() {
		var wg sync.WaitGroup
		for i := 0; i < 4000; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				for k, v := range kvs {
					rv, rerr := cache.Get(nil, k)
					require.NoError(rerr)
					require.Equal(v, rv.(string))
				}
			}()
			go func() {
				defer wg.Done()
				for k, err := range kerrs {
					rv, rerr := cache.Get(nil, k)
					require.Equal(err, rerr)
					require.Equal("", rv.(string))
				}
			}()
		}
		wg.Wait()
	}

	// And now the test begins...

	expectVals()
	expectErrs()
	get()

	// Still within cleanup interval.
	clk.Add(5 * time.Second)
	get()

	// Trigger cleanup but nothing has expired.
	clk.Add(10 * time.Second)
	get()

	// Trigger cleanup which should only expire the errors.
	clk.Add(time.Minute)
	expectErrs()
	get()

	// Trigger cleanup right before the errors expire...
	clk.Add(59 * time.Second)
	get()
	// Then a get after errors have expired should not run cleanup since still
	// within interval.
	clk.Add(2 * time.Second)
	get()

	// Trigger cleanup which should expire both errors and values.
	clk.Add(time.Hour)
	expectVals()
	expectErrs()
	get()

	// Final get should have no effect.
	get()
}
