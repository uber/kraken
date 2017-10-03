package httputil

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"time"
)

const defaultTimeout = 60 * time.Second

// StatusError occurs if an HTTP response has an unexpected status code.
type StatusError struct {
	Method           string
	URL              string
	ExpectedStatuses []int
	Status           int
	ResponseDump     string
}

func (e StatusError) Error() string {
	return fmt.Sprintf(
		"http request \"%s %s\" failed: expected statuses %v, got status %d: %s",
		e.Method, e.URL, e.ExpectedStatuses, e.Status, e.ResponseDump)
}

type sendOptions struct {
	body          io.Reader
	timeout       time.Duration
	acceptedCodes map[int]struct{}
	headers       map[string]string
}

// defaultSendOptions creates httpOptions with default settings
func defaultSendOptions() sendOptions {
	return sendOptions{
		body:          bytes.NewReader([]byte{}),
		timeout:       defaultTimeout,
		acceptedCodes: map[int]struct{}{http.StatusOK: {}},
		headers:       map[string]string{},
	}
}

// SendOption specifies options for http request
// it overwrites the default value in httpOptions
type SendOption struct {
	f func(*sendOptions)
}

// SendBody specifies a body for http request
func SendBody(body io.Reader) SendOption {
	return SendOption{func(opts *sendOptions) {
		opts.body = body
	}}
}

// SendTimeout specifies timeout for http request
func SendTimeout(t time.Duration) SendOption {
	return SendOption{func(opts *sendOptions) {
		opts.timeout = t
	}}
}

// SendHeaders specifies headers for http request
func SendHeaders(headers map[string]string) SendOption {
	return SendOption{func(opts *sendOptions) {
		opts.headers = headers
	}}
}

// SendAcceptedCodes specifies accepted codes for http request
func SendAcceptedCodes(codes ...int) SendOption {
	m := make(map[int]struct{})
	for _, c := range codes {
		m[c] = struct{}{}
	}
	return SendOption{func(opts *sendOptions) {
		opts.acceptedCodes = m
	}}
}

// Send sends an http request.
func Send(method, url string, options ...SendOption) (*http.Response, error) {
	opts := defaultSendOptions()
	for _, opt := range options {
		opt.f(&opts)
	}

	req, err := http.NewRequest(method, url, opts.body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %s", err)
	}

	for key, val := range opts.headers {
		req.Header.Set(key, val)
	}

	client := http.Client{
		Timeout: opts.timeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %s", err)
	}

	_, ok := opts.acceptedCodes[resp.StatusCode]
	if !ok {
		var expected []int
		for code := range opts.acceptedCodes {
			expected = append(expected, code)
		}

		defer resp.Body.Close()
		respBytes, err := httputil.DumpResponse(resp, true)
		respDump := string(respBytes)
		if err != nil {
			respDump = fmt.Sprintf("failed to dump response: %s", err)
		}

		return nil, StatusError{
			Method:           method,
			URL:              url,
			ExpectedStatuses: expected,
			Status:           resp.StatusCode,
			ResponseDump:     respDump,
		}
	}

	return resp, nil
}

// Get sends a GET http request.
func Get(url string, options ...SendOption) (*http.Response, error) {
	return Send("GET", url, options...)
}

// Head sends a HEAD http request.
func Head(url string, options ...SendOption) (*http.Response, error) {
	return Send("HEAD", url, options...)
}

// Post sends a POST http request.
func Post(url string, options ...SendOption) (*http.Response, error) {
	return Send("POST", url, options...)
}

// Put sends a PUT http request.
func Put(url string, options ...SendOption) (*http.Response, error) {
	return Send("PUT", url, options...)
}

// Patch sends a PATCH http request.
func Patch(url string, options ...SendOption) (*http.Response, error) {
	return Send("PATCH", url, options...)
}

// Delete sends a DELETE http request.
func Delete(url string, options ...SendOption) (*http.Response, error) {
	return Send("DELETE", url, options...)
}
