package dockerregistry

import "os"

// ChanReadCloser is a readcloser with a channel
type ChanReadCloser struct {
	Chan chan byte
	f    *os.File
}

// Read reads
func (rc ChanReadCloser) Read(p []byte) (n int, err error) {
	return rc.f.Read(p)
}

// Close sends a signal to the channel and close the reader
func (rc ChanReadCloser) Close() error {
	select {
	case rc.Chan <- uint8(1):
		break
	default:
	}

	return rc.f.Close()
}
