package handler

import (
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/utils/log"
)

// Error defines an HTTP handler error which encapsulates status and headers
// to be set in the HTTP response.
type Error struct {
	status int
	header http.Header
	msg    string
}

// Errorf creates a new Error with Printf-style formatting. Defaults to 500 error.
func Errorf(format string, args ...interface{}) *Error {
	return &Error{
		status: http.StatusInternalServerError,
		header: http.Header{},
		msg:    fmt.Sprintf(format, args...),
	}
}

// ErrorStatus creates an empty message error with status s.
func ErrorStatus(s int) *Error {
	return Errorf("").Status(s)
}

// Status sets a custom status on e.
func (e *Error) Status(s int) *Error {
	e.status = s
	return e
}

// Header adds a custom header to e.
func (e *Error) Header(k, v string) *Error {
	e.header.Add(k, v)
	return e
}

// GetStatus returns the error status.
func (e *Error) GetStatus() int {
	return e.status
}

func (e *Error) Error() string {
	if e.msg == "" {
		return fmt.Sprintf("server error %d", e.status)
	}
	return fmt.Sprintf("server error %d: %s", e.status, e.msg)
}

// ErrHandler defines an HTTP handler which returns an error.
type ErrHandler func(http.ResponseWriter, *http.Request) error

// Wrap converts an ErrHandler into an http.HandlerFunc by handling the error
// returned by h.
func Wrap(h ErrHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var status int
		var errMsg string
		if err := h(w, r); err != nil {
			switch e := err.(type) {
			case *Error:
				for k, vs := range e.header {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				status = e.status
				errMsg = e.msg
			default:
				status = http.StatusInternalServerError
				errMsg = e.Error()
			}
			w.WriteHeader(status)
			w.Write([]byte(errMsg))
		} else {
			status = http.StatusOK
		}
		log.Infof("%d %s %s %s", status, r.Method, r.URL.Path, errMsg)
	}
}
