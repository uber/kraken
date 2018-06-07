package hdfsbackend

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/lib/backend/namepath"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

var errAllNameNodesUnavailable = errors.New(
	"exhausted the list of name nodes for the request without success")

func retryable(err error) bool {
	return httputil.IsForbidden(err) || httputil.IsNetworkError(err)
}

// Client is a backend.Client for HDFS.
type Client struct {
	config Config
	pather namepath.Pather
}

// NewClient returns a new Client.
func NewClient(config Config) (*Client, error) {
	config = config.applyDefaults()
	if len(config.NameNodes) == 0 {
		return nil, errors.New("namenodes required")
	}
	pather, err := namepath.New(config.RootDirectory, config.NamePath)
	if err != nil {
		return nil, fmt.Errorf("namepath: %s", err)
	}
	return &Client{config, pather}, nil
}

type fileStatusResponse struct {
	FileStatus struct {
		Length int64 `json:"length"`
	} `json:"FileStatus"`
}

// Stat returns blob info for name.
func (c *Client) Stat(name string) (*blobinfo.Info, error) {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return nil, fmt.Errorf("blob path: %s", err)
	}

	v := url.Values{}
	v.Set("op", "GETFILESTATUS")
	c.setUserName(v)

	for _, node := range c.config.NameNodes {
		resp, err := httputil.Get(fmt.Sprintf("http://%s/%s?%s", node, path, v.Encode()))
		if err != nil {
			if retryable(err) {
				continue
			}
			if httputil.IsNotFound(err) {
				return nil, backenderrors.ErrBlobNotFound
			}
			return nil, err
		}
		defer resp.Body.Close()
		var fsr fileStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&fsr); err != nil {
			return nil, fmt.Errorf("decode body: %s", err)
		}
		return blobinfo.New(fsr.FileStatus.Length), nil
	}
	return nil, errAllNameNodesUnavailable
}

// Download downloads name into dst.
func (c *Client) Download(name string, dst io.Writer) error {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}

	v := url.Values{}
	v.Set("op", "OPEN")
	c.setUserName(v)
	c.setBuffersize(v)

	for _, node := range c.config.NameNodes {
		resp, err := httputil.Get(fmt.Sprintf("http://%s/%s?%s", node, path, v.Encode()))
		if err != nil {
			if retryable(err) {
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

// Upload uploads src to name.
func (c *Client) Upload(name string, src io.Reader) error {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}

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

	v := url.Values{}
	v.Set("op", "CREATE")
	c.setUserName(v)
	c.setBuffersize(v)
	v.Set("overwrite", "true")

	for _, node := range c.config.NameNodes {
		nameresp, err := httputil.Put(
			fmt.Sprintf("http://%s/%s?%s", node, path, v.Encode()),
			httputil.SendRedirect(func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			}),
			httputil.SendAcceptedCodes(http.StatusTemporaryRedirect, http.StatusPermanentRedirect))
		if err != nil {
			if retryable(err) {
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

type listResponse struct {
	FileStatuses struct {
		FileStatus []struct {
			PathSuffix string `json:"pathSuffix"`
		} `json:"FileStatus"`
	} `json:"FileStatuses"`
}

// List lists names in dir.
func (c *Client) List(dir string) ([]string, error) {
	path, err := c.pather.DirPath(dir)
	if err != nil {
		return nil, fmt.Errorf("dir path: %s", err)
	}

	v := url.Values{}
	v.Set("op", "LISTSTATUS")
	c.setUserName(v)

	for _, node := range c.config.NameNodes {
		resp, err := httputil.Get(fmt.Sprintf("http://%s/%s?%s", node, path, v.Encode()))
		if err != nil {
			if retryable(err) {
				continue
			}
			if httputil.IsNotFound(err) {
				return nil, backenderrors.ErrDirNotFound
			}
			return nil, err
		}
		defer resp.Body.Close()
		var lr listResponse
		if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
			return nil, fmt.Errorf("decode body: %s", err)
		}
		var names []string
		for _, fs := range lr.FileStatuses.FileStatus {
			names = append(names, fs.PathSuffix)
		}
		return names, nil
	}
	return nil, errAllNameNodesUnavailable
}

func (c *Client) setBuffersize(v url.Values) {
	v.Set("buffersize", strconv.FormatInt(c.config.BuffSize, 10))
}

func (c *Client) setUserName(v url.Values) {
	if c.config.UserName != "" {
		v.Set("user.name", c.config.UserName)
	}
}
