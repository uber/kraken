package trackerserver

import "net/http"

type healthHandler struct{}

func (h *healthHandler) Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("OK ;-)\n"))
}
