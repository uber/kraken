package hdfsbackend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

var errAllNameNodesUnavailable = errors.New(
	"exhausted the list of name nodes for the request without success")

func retryable(err error) bool {
	return httputil.IsForbidden(err) || httputil.IsNetworkError(err)
}

type client struct {
	config Config
}

func newClient(config Config) *client {
	return &client{config}
}

func (c *client) download(path string, dst io.Writer) error {
	params := c.params("open")
	for _, node := range c.config.NameNodes {
		u := fmt.Sprintf("http://%s/%s?%s", node, path, params)
		log.Infof("Starting HDFS download from %s", u)
		resp, err := httputil.Get(u)
		if err != nil {
			if retryable(err) {
				log.Infof("HDFS download error: %s, retrying from the next name node", err)
				continue
			}
			if httputil.IsNotFound(err) {
				return backenderrors.ErrBlobNotFound
			}
			return err
		}
		if n, err := io.Copy(dst, resp.Body); err != nil {
			return fmt.Errorf("copy response: %s", err)
		} else if n != resp.ContentLength {
			return fmt.Errorf(
				"transferred bytes %d does not match content length %d", n, resp.ContentLength)
		}
		return nil
	}
	return errAllNameNodesUnavailable
}

type exceededCapError error

// capBuffer is a buffer that returns errors if the buffer exceeds cap.
type capBuffer struct {
	cap int64
	buf *bytes.Buffer
}

func (b *capBuffer) Write(p []byte) (n int, err error) {
	if int64(len(p)+b.buf.Len()) > b.cap {
		return 0, exceededCapError(
			fmt.Errorf("buffer exceeded max capacity %s", memsize.Format(uint64(b.cap))))
	}
	return b.buf.Write(p)
}

type drainSrcError struct {
	err error
}

func (e drainSrcError) Error() string { return fmt.Sprintf("drain src: %s", e.err) }

func (c *client) upload(path string, src io.Reader) error {

	// We must be able to replay src in the event that uploading to the data node
	// fails halfway through the upload, thus we attempt to upcast src to an io.Seeker
	// for this purpose. If src is not an io.Seeker, we drain it to an in-memory buffer
	// that can be replayed.
	readSeeker, ok := src.(io.ReadSeeker)
	if !ok {
		var b []byte
		if buf, ok := src.(*bytes.Buffer); ok {
			// Optimization to avoid draining an existing buffer.
			b = buf.Bytes()
		} else {
			log.With("path", path).Info("Draining HDFS upload source into replayable buffer")
			cbuf := &capBuffer{int64(c.config.BufferGuard), new(bytes.Buffer)}
			if _, err := io.Copy(cbuf, src); err != nil {
				return drainSrcError{err}
			}
			b = cbuf.buf.Bytes()
		}
		readSeeker = bytes.NewReader(b)
	}

	params := c.params("create")
	for _, node := range c.config.NameNodes {
		nameresp, err := httputil.Put(
			fmt.Sprintf("http://%s/%s?%s", node, path, params),
			httputil.SendRedirect(func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			}),
			httputil.SendAcceptedCodes(http.StatusTemporaryRedirect, http.StatusPermanentRedirect))
		if err != nil {
			if retryable(err) {
				log.Infof("HDFS upload error: %s, retrying from the next name node", err)
				continue
			}
			return err
		}
		defer nameresp.Body.Close()

		// Follow redirect location manually per WebHDFS protocol.
		loc, ok := nameresp.Header["Location"]
		if !ok || len(loc) == 0 {
			return fmt.Errorf("missing location field in response header: %s", nameresp.Header)
		}

		dataresp, err := httputil.Put(
			loc[0],
			httputil.SendBody(readSeeker),
			httputil.SendAcceptedCodes(http.StatusCreated))
		if err != nil {
			if retryable(err) {
				log.Infof("HDFS upload error: %s, retrying from the next name node", err)
				// Reset reader for next retry.
				if _, err := readSeeker.Seek(0, io.SeekStart); err != nil {
					return fmt.Errorf("seek: %s", err)
				}
				continue
			}
			return err
		}
		defer dataresp.Body.Close()

		return nil
	}
	return errAllNameNodesUnavailable
}

func (c *client) params(op string) string {
	v := url.Values{}
	if c.config.UserName != "" {
		v.Set("user.name", c.config.UserName)
	}
	v.Set("buffersize", strconv.FormatInt(c.config.BuffSize, 10))
	v.Set("overwrite", "true")
	v.Set("op", op)
	return v.Encode()
}
