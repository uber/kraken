package httputil

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

const defaultTimeout = 60 * time.Second

// StatusError occurs if an HTTP response has an unexpected status code.
type StatusError struct {
	Method           string
	URL              string
	ExpectedStatuses []int
	Status           int
	Header           http.Header

	ResponseDump string
}

func newStatusError(
	method string, url string, expectedStatuses []int, resp *http.Response) StatusError {

	defer resp.Body.Close()
	respBytes, err := ioutil.ReadAll(resp.Body)
	respDump := string(respBytes)
	if err != nil {
		respDump = fmt.Sprintf("failed to dump response: %s", err)
	}

	return StatusError{
		Method:           method,
		URL:              url,
		ExpectedStatuses: expectedStatuses,
		Status:           resp.StatusCode,
		Header:           resp.Header,
		ResponseDump:     respDump,
	}
}

func (e StatusError) Error() string {
	return fmt.Sprintf(
		"http request \"%s %s\" failed: expected statuses %v, got status %d: %s",
		e.Method, e.URL, e.ExpectedStatuses, e.Status, e.ResponseDump)
}

// IsStatus returns true if err is a StatusError of the given status.
func IsStatus(err error, status int) bool {
	statusErr, ok := err.(StatusError)
	return ok && statusErr.Status == status
}

// IsCreated returns true if err is a "created", 201
func IsCreated(err error) bool {
	return IsStatus(err, http.StatusCreated)
}

// IsNotFound returns true if err is a "not found" StatusError.
func IsNotFound(err error) bool {
	return IsStatus(err, http.StatusNotFound)
}

// IsConflict returns true if err is a "status conflict" StatusError.
func IsConflict(err error) bool {
	return IsStatus(err, http.StatusConflict)
}

// IsAccepted returns true if err is a "status accepted" StatusError.
func IsAccepted(err error) bool {
	return IsStatus(err, http.StatusAccepted)
}

// NetworkError occurs on any Send error which occurred while trying to send
// the HTTP request, e.g. the given host is unresponsive.
type NetworkError struct {
	method string
	url    string
	err    error
}

func (e NetworkError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.method, e.url, e.err)
}

type sendOptions struct {
	body           io.Reader
	timeout        time.Duration
	acceptedCodes  map[int]struct{}
	headers        map[string]string
	redirectPolicy func(req *http.Request, via []*http.Request) error
	client         *http.Client
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

// SendRedirect specifies a redirect policy for http request
func SendRedirect(redirect func(req *http.Request, via []*http.Request) error) SendOption {
	return SendOption{func(opts *sendOptions) {
		opts.redirectPolicy = redirect
	}}
}

// Send sends an HTTP request. May return NetworkError or StatusError (see above).
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
		Timeout:       opts.timeout,
		CheckRedirect: opts.redirectPolicy,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, NetworkError{method, url, err}
	}

	_, ok := opts.acceptedCodes[resp.StatusCode]
	if !ok {
		var expected []int
		for code := range opts.acceptedCodes {
			expected = append(expected, code)
		}
		return resp, newStatusError(method, url, expected, resp)
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
