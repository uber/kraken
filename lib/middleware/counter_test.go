package middleware

import (
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatsCounterMiddleware(t *testing.T) {
	require := require.New(t)

	stats := tally.NewTestScope("testing", nil)

	r := chi.NewRouter()

	r.Use(Counter(stats.SubScope("a.c")))
	r.Get("/a/:b/c/:d", func(w http.ResponseWriter, r *http.Request) {})

	r.Group(func(r chi.Router) {
		r.Use(Counter(stats.SubScope("b.d")))
		r.Post("/b/d/:param", func(w http.ResponseWriter, r *http.Request) {})

	})
	s := httptest.NewServer(r)
	defer s.Close()

	require.Equal(stats.Snapshot().Counters()["testing.a.c.GET.requests"], nil)

	_, err := http.Get("http://" + s.Listener.Addr().String() + "/a/param1/c/param2")
	require.NoError(err)

	_, err = http.Post("http://"+s.Listener.Addr().String()+"/b/d/param", "", nil)
	require.NoError(err)

	require.Equal(stats.Snapshot().Counters()["testing.a.c.GET.requests"].Value(), int64(1))
	require.Equal(stats.Snapshot().Counters()["testing.b.d.POST.requests"].Value(), int64(1))

	_, err = http.Get("http://" + s.Listener.Addr().String() + "/a/param1/c/param2")

	require.NoError(err)
	require.Equal(stats.Snapshot().Counters()["testing.a.c.GET.requests"].Value(), int64(2))

}
