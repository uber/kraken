package httputil

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"time"
)

const (
	defaultTimeout = 60 * time.Second
)

type sendError struct {
	url    string
	method string
	msg    string
}

func (e sendError) Error() string {
	return fmt.Sprintf("error sending http request to url: %s, method: %s, err: %s", e.url, e.method, e.msg)
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
func SendAcceptedCodes(codes []int) SendOption {
	m := make(map[int]struct{})
	for _, c := range codes {
		m[c] = struct{}{}
	}
	return SendOption{func(opts *sendOptions) {
		opts.acceptedCodes = m
	}}
}

// Send sends http request
func Send(method, endpoint string, options ...SendOption) (*http.Response, error) {
	opts := defaultSendOptions()
	for _, opt := range options {
		opt.f(&opts)
	}

	req, err := http.NewRequest(method, endpoint, opts.body)
	if err != nil {
		return nil, err
	}

	for key, val := range opts.headers {
		req.Header.Set(key, val)
	}

	client := http.Client{
		Timeout: opts.timeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	_, ok := opts.acceptedCodes[resp.StatusCode]
	if !ok {
		defer resp.Body.Close()
		respDump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return nil, sendError{endpoint, method, fmt.Sprintf("unexpected response code %d and failed to parse body: %s", resp.StatusCode, err)}
		}
		return nil, sendError{endpoint, method, string(respDump)}
	}

	return resp, nil
}
