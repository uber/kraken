package middleware

import (
	"github.com/uber-go/tally"
	"net/http"
)

// Counter measures endpoint's hit count
func Counter(stats tally.Scope) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		middlewarefn := func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			stats.SubScope(r.Method).Counter("requests").Inc(1)
		}
		return http.HandlerFunc(middlewarefn)
	}
}
