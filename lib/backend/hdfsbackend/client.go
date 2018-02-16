package hdfsbackend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
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
		n, err := io.Copy(dst, resp.Body)
		if err != nil {
			return fmt.Errorf("copy response: %s", err)
		}
		if n != resp.ContentLength {
			return fmt.Errorf(
				"transfered bytes %d does not match content length %d", n, resp.ContentLength)
		}
		return nil
	}
	return errAllNameNodesUnavailable
}

func (c *client) upload(path string, src io.Reader) error {

	// We must be able to replay src in the event that uploading to the data node
	// fails halfway through the upload, thus we attempt to upcast src to a Seeker
	// for this purpose. If src is not a Seeker, we drain it to an in-memory buffer
	// that can be replayed.
	readSeeker, ok := src.(io.ReadSeeker)
	if !ok {
		log.With("path", path).Info("Draining HDFS upload source into replayable buffer")
		b, err := ioutil.ReadAll(src)
		if err != nil {
			return fmt.Errorf("drain src: %s", err)
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
