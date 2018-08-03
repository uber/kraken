package hdfsbackend

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"sync"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
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

// Stat returns blob info for name.
func (c *Client) Stat(name string) (*core.BlobInfo, error) {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return nil, fmt.Errorf("blob path: %s", err)
	}
	fs, err := c.getFileStatus(path)
	if err != nil {
		return nil, err
	}
	return core.NewBlobInfo(fs.Length), nil
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

var (
	_ignoreRegex = regexp.MustCompile(
		"^.+/repositories/.+/(_layers|_uploads|_manifests/(revisions|tags/.+/index)).*")
	_stopRegex = regexp.MustCompile("^.+/repositories/.+/_manifests$")
)

// List lists names which start with prefix.
func (c *Client) List(prefix string) ([]string, error) {
	root := path.Join(c.pather.BasePath(), prefix)

	var wg sync.WaitGroup
	listJobs := make(chan string, c.config.ListConcurrency)
	errc := make(chan error, c.config.ListConcurrency)

	wg.Add(1)
	listJobs <- root

	go func() {
		wg.Wait()
		close(listJobs)
	}()

	var mu sync.Mutex
	var files []string

L:
	for {
		select {
		case err := <-errc:
			// Stop early on error.
			return nil, err
		case dir, ok := <-listJobs:
			if !ok {
				break L
			}
			go func() {
				defer wg.Done()

				contents, err := c.listFileStatus(dir)
				if err != nil {
					if !httputil.IsNotFound(err) {
						errc <- err
					}
					return
				}

				for _, fs := range contents {
					p := path.Join(dir, fs.PathSuffix)

					// TODO(codyg): This is an ugly hack to avoid walking through non-tags
					// during Docker catalog. Ideally, only tags are located in the repositories
					// directory, however in WBU2 HDFS, there are blobs here as well. At some
					// point, we must migrate the data into a structure which cleanly divides
					// blobs and tags (like we do in S3).
					if _ignoreRegex.MatchString(p) {
						continue
					}

					// TODO(codyg): Another ugly hack to speed up catalog performance by stopping
					// early when we hit tags...
					if _stopRegex.MatchString(p) {
						p = path.Join(p, "tags/dummy/current/link")
						fs.Type = "FILE"
					}

					if fs.Type == "DIRECTORY" {
						wg.Add(1)
						listJobs <- p
						continue
					}

					name, err := c.pather.NameFromBlobPath(p)
					if err != nil {
						log.With("path", p).Errorf("Error converting blob path into name: %s", err)
						continue
					}
					mu.Lock()
					files = append(files, name)
					mu.Unlock()
				}
			}()
		}
	}

	return files, nil
}

func (c *Client) getFileStatus(path string) (fileStatus, error) {
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
				return fileStatus{}, backenderrors.ErrBlobNotFound
			}
			return fileStatus{}, err
		}
		defer resp.Body.Close()
		var fsr fileStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&fsr); err != nil {
			return fileStatus{}, fmt.Errorf("decode body: %s", err)
		}
		return fsr.FileStatus, nil
	}
	return fileStatus{}, errAllNameNodesUnavailable
}

func (c *Client) listFileStatus(path string) ([]fileStatus, error) {
	v := url.Values{}
	v.Set("op", "LISTSTATUS")
	c.setUserName(v)

	for _, node := range c.config.NameNodes {
		resp, err := httputil.Get(fmt.Sprintf("http://%s/%s?%s", node, path, v.Encode()))
		if err != nil {
			if retryable(err) {
				continue
			}
			return nil, err
		}
		defer resp.Body.Close()
		var lsr listStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&lsr); err != nil {
			return nil, fmt.Errorf("decode body: %s", err)
		}
		return lsr.FileStatuses.FileStatus, nil
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
