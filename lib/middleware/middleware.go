package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/chi"
	"github.com/uber-go/tally"
)

// tagEndpoint tags stats by endpoint path and method, ignoring any path variables.
// For example, "/foo/:foo/bar/:bar" is tagged with endpoint "foo.bar"
//
// Note: tagEndpoint should always be called AFTER the "next" handler serves,
// such that chi can populate proper route context with the path.
//
// Wrong:
//
//     tagEndpoint(stats, r).Counter("n").Inc(1)
//     next.ServeHTTP(w, r)
//
// Right:
//
//     next.ServeHTTP(w, r)
//     tagEndpoint(stats, r).Counter("n").Inc(1)
//
func tagEndpoint(stats tally.Scope, r *http.Request) tally.Scope {
	ctx := chi.RouteContext(r.Context())
	var staticParts []string
	for _, part := range strings.Split(ctx.RoutePattern, "/") {
		if len(part) == 0 || part[0] == ':' {
			continue
		}
		staticParts = append(staticParts, part)
	}
	return stats.Tagged(map[string]string{
		"endpoint": strings.Join(staticParts, "."),
		"method":   strings.ToUpper(r.Method),
	})
}

// LatencyTimer measures endpoint latencies.
func LatencyTimer(stats tally.Scope) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			tagEndpoint(stats, r).Timer("latency").Record(time.Since(start))
		})
	}
}

type recordStatusWriter struct {
	http.ResponseWriter
	wroteHeader bool
	code        int
}

func (w *recordStatusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code = code
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *recordStatusWriter) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.ResponseWriter.Write(b)
}

// StatusCounter measures endpoint status count.
func StatusCounter(stats tally.Scope) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recordw := &recordStatusWriter{w, false, http.StatusOK}
			next.ServeHTTP(recordw, r)
			tagEndpoint(stats, r).Counter(strconv.Itoa(recordw.code)).Inc(1)
		})
	}
}
