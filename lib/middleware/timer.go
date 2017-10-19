package middleware

import (
	"github.com/uber-go/tally"
	"net/http"
	"time"
)

// ElapsedTimer measures endpoint's latencies
func ElapsedTimer(stats tally.Scope) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		middlewarefn := func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()
			next.ServeHTTP(w, r)
			elapsed := time.Since(start)
			stats.SubScope(r.Method).Timer("request_time").Record(elapsed)
		}
		return http.HandlerFunc(middlewarefn)
	}
}
