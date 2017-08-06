package service

import "fmt"

// ServerError represents http errors.
type ServerError struct {
	statusCode int
	msg        string
}

// StatusCode returns 400.
func (e *ServerError) StatusCode() int {
	return e.statusCode
}

func (e *ServerError) Error() string {
	return e.msg
}

// NewServerError init a ServerError obj.
func NewServerError(code int, format string, args ...interface{}) *ServerError {
	return &ServerError{
		statusCode: code,
		msg:        fmt.Sprintf(format, args...),
	}
}
