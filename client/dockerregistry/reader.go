package dockerregistry

import "os"

// ChanReadCloser is a readcloser with a channel
type ChanReadCloser struct {
	f *os.File
}

// Read reads
func (rc ChanReadCloser) Read(p []byte) (n int, err error) {
	return rc.f.Read(p)
}

// Close sends a signal to the channel and close the reader
func (rc ChanReadCloser) Close() error {
	return rc.f.Close()
}
