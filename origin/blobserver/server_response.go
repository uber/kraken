package blobserver

import "fmt"

// ServerResponse encapsulates http response headers.
type ServerResponse struct {
	statusCode int
	headers    map[string][]string
	errMsg     string
}

// NewServerResponse init a ServerResponse obj.
func NewServerResponse(code int) *ServerResponse {
	return &ServerResponse{
		statusCode: code,
		headers:    map[string][]string{},
		errMsg:     "",
	}
}

// NewServerResponseWithError init a ServerResponse obj with error message.
func NewServerResponseWithError(code int, format string, args ...interface{}) *ServerResponse {
	return &ServerResponse{
		statusCode: code,
		headers:    map[string][]string{},
		errMsg:     fmt.Sprintf(format, args...),
	}
}

// AddHeader adds the key value pair to HTTP response header.
// It appends the value if key exists.
func (s *ServerResponse) AddHeader(key, value string) {
	values, ok := s.headers[key]
	if !ok {
		s.headers[key] = []string{value}
	} else {
		s.headers[key] = append(values, value)
	}
}

// SetHeader sets header associated with key to the single element value.
func (s *ServerResponse) SetHeader(key, value string) {
	s.headers[key] = []string{value}
}

// GetHeaders returns the header key value map.
func (s *ServerResponse) GetHeaders() map[string][]string {
	return s.headers
}

// GetStatusCode sets status code in HTTP response.
func (s *ServerResponse) GetStatusCode() int {
	return s.statusCode
}

// Error returns a error messge to be logged and included in HTTP reponse body.
func (s *ServerResponse) Error() string {
	return s.errMsg
}
