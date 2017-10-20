package middleware

import (
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStatsElapsedTimerMiddleware(t *testing.T) {
	require := require.New(t)

	stats := tally.NewTestScope("testing", nil)

	r := chi.NewRouter()

	r.Use(ElapsedTimer(stats.SubScope("a.c")))
	r.Get("/a/:b/c/:d", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	})
	r.Group(func(r chi.Router) {
		r.Use(ElapsedTimer(stats.SubScope("b.d")))
		r.Post("/b/d/:param", func(w http.ResponseWriter, r *http.Request) {})

	})

	s := httptest.NewServer(r)
	defer s.Close()

	require.Equal(stats.Snapshot().Timers()["testing.a.c.GET.request_time"], nil)

	_, err := http.Get("http://" + s.Listener.Addr().String() + "/a/param1/c/param2")

	require.NoError(err)

	tnow := time.Now()
	delta := stats.Snapshot().Timers()["testing.a.c.GET.request_time"].Values()[0]
	require.WithinDuration(tnow, tnow.Add(delta), 500*time.Millisecond)

	_, err = http.Get("http://" + s.Listener.Addr().String() + "/a/param1/c/param2")
	require.NoError(err)

	_, err = http.Post("http://"+s.Listener.Addr().String()+"/b/d/param", "", nil)
	require.NoError(err)

	tnow = time.Now()
	delta = stats.Snapshot().Timers()["testing.a.c.GET.request_time"].Values()[1]
	require.WithinDuration(tnow, tnow.Add(delta), 500*time.Millisecond)

	tnow = time.Now()
	delta = stats.Snapshot().Timers()["testing.b.d.POST.request_time"].Values()[0]
	require.WithinDuration(tnow, tnow.Add(delta), 500*time.Millisecond)
}
