package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/pressly/chi"
	"github.com/uber-go/tally"
)

// scopeByEndpoint scopes stats by path and method, ignoring any path variables.
// For example, "GET /foo/:foo/bar/:bar" is converted into the scope "foo.bar.GET".
//
// Note: scopeByEndpoint should always be called AFTER the "next" handler serves,
// such that chi can populate proper route context with the path.
//
// Wrong:
//
//     scopeByEndpoint(stats, r).Counter("n").Inc(1)
//     next.ServeHTTP(w, r)
//
// Right:
//
//     next.ServeHTTP(w, r)
//     scopeByEndpoint(stats, r).Counter("n").Inc(1)
//
func scopeByEndpoint(stats tally.Scope, r *http.Request) tally.Scope {
	ctx := chi.RouteContext(r.Context())
	for _, part := range strings.Split(ctx.RoutePattern, "/") {
		if len(part) == 0 || part[0] == ':' {
			continue
		}
		stats = stats.SubScope(part)
	}
	stats = stats.SubScope(strings.ToUpper(r.Method))
	return stats
}

// LatencyTimer measures endpoint latencies.
func LatencyTimer(stats tally.Scope) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			scopeByEndpoint(stats, r).Timer("latency").Record(time.Since(start))
		})
	}
}

// HitCounter measures endpoint hit count.
func HitCounter(stats tally.Scope) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			scopeByEndpoint(stats, r).Counter("count").Inc(1)
		})
	}
}
