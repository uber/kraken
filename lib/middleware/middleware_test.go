package middleware

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestScopeByEndpoint(t *testing.T) {
	tests := []struct {
		method   string
		path     string
		reqPath  string
		expected string
	}{
		{"GET", "/foo/:foo/bar/:bar", "/foo/x/bar/y", "foo.bar.GET"},
		{"POST", "/foo/:foo/bar/:bar", "/foo/x/bar/y", "foo.bar.POST"},
		{"GET", "/a/b/c", "/a/b/c", "a.b.c.GET"},
		{"GET", "/", "/", "GET"},
		{"GET", "/x/:a/:b/:c", "/x/a/b/c", "x.GET"},
	}

	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			require := require.New(t)

			stats := tally.NewTestScope("", nil)

			r := chi.NewRouter()
			r.HandleFunc(test.path, func(w http.ResponseWriter, r *http.Request) {
				scopeByEndpoint(stats, r).Counter("count").Inc(1)
			})
			addr, stop := testutil.StartServer(r)
			defer stop()

			_, err := httputil.Send(test.method, fmt.Sprintf("http://%s%s", addr, test.reqPath))
			require.NoError(err)

			counter, ok := stats.Snapshot().Counters()[test.expected+".count"]
			require.True(ok)
			require.Equal(int64(1), counter.Value())
		})
	}
}

func TestLatencyTimer(t *testing.T) {
	require := require.New(t)

	stats := tally.NewTestScope("", nil)

	r := chi.NewRouter()
	r.Use(LatencyTimer(stats))
	r.Get("/foo/:foo", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	})

	addr, stop := testutil.StartServer(r)
	defer stop()

	_, err := httputil.Get(fmt.Sprintf("http://%s/foo/x", addr))
	require.NoError(err)

	now := time.Now()

	timer, ok := stats.Snapshot().Timers()["foo.GET.latency"]
	require.True(ok)
	require.WithinDuration(now, now.Add(timer.Values()[0]), 500*time.Millisecond)
}

func TestHitCounter(t *testing.T) {
	require := require.New(t)

	stats := tally.NewTestScope("", nil)

	r := chi.NewRouter()
	r.Use(HitCounter(stats))
	r.Get("/foo/:foo", func(w http.ResponseWriter, r *http.Request) {})

	addr, stop := testutil.StartServer(r)
	defer stop()

	for i := 0; i < 5; i++ {
		_, err := httputil.Get(fmt.Sprintf("http://%s/foo/x", addr))
		require.NoError(err)
	}

	counter, ok := stats.Snapshot().Counters()["foo.GET.count"]
	require.True(ok)
	require.Equal(int64(5), counter.Value())
}
