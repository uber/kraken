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

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/fileio"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config manages HDFS connection parameters,
// host:port, authentication info,
// token, etc
type Config struct {
	NameNodes []string `yaml:"namenodes"`
	BuffSize  int64    `yaml:"buff_size"` // default transfer block size
	UserName  string   `yaml:"username"`  // auth username
}

const (
	// HDFS WebDAV operations, there will be more
	// currently we only support file reads

	// HdfsOpen defines open operator
	HdfsOpen = "open"

	// HdfsCreate defines create operator
	HdfsCreate = "create"
)

var errAllNameNodesUnavailable = errors.New(
	"exhausted the list of name nodes for the request without success")

// Client implements HDFS file upload/download functionality
type Client struct {
	config Config
}

func (c Config) applyDefaults() Config {
	if c.BuffSize == 0 {
		c.BuffSize = int64(64 * memsize.MB)
	}

	return c
}

// NewHDFSClient creates a client to HDFS cluster
func NewHDFSClient(config Config) (*Client, error) {
	if len(config.NameNodes) == 0 {
		return nil, fmt.Errorf("empty namenodes config")
	}
	return &Client{config: config.applyDefaults()}, nil
}

// create url parameters for the call based on configuration
// and operation
func (c *Client) createParams(op string) url.Values {
	v := url.Values{}

	if c.config.UserName != "" {
		v.Set("user.name", c.config.UserName)
	}

	v.Set("buffersize", strconv.FormatInt(c.config.BuffSize, 10))
	v.Set("overwrite", "true")
	v.Set("op", op)

	return v
}

// download returns http reader to a file from HDFS datastore
func (c *Client) download(path string) (io.ReadCloser, int64, error) {
	for _, nn := range c.config.NameNodes {

		url := "http://" + nn + path
		resp, err := httputil.Get(url)
		if err != nil {
			// if 403 or network error retry from a different namenode
			if httputil.IsForbidden(err) || httputil.IsNetworkError(err) {
				log.Infof("HDFS download error: %s, retrying from the next name node", err)
				continue
			}

			if httputil.IsNotFound(err) {
				return nil, -1, backenderrors.ErrBlobNotFound
			}
			return nil, -1, fmt.Errorf("could not get a content from hdfs: %s", err)
		}
		return resp.Body, resp.ContentLength, nil
	}
	return nil, -1, errAllNameNodesUnavailable
}

// upload writes stream from input reader to HDFS datastore
func (c *Client) upload(path string, r io.Reader) error {
	for _, nn := range c.config.NameNodes {
		url := "http://" + nn + path

		nameresp, err := httputil.Put(
			url,
			httputil.SendRedirect(
				func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				},
			),
			httputil.SendAcceptedCodes(http.StatusTemporaryRedirect,
				http.StatusPermanentRedirect),
		)
		defer nameresp.Body.Close()

		if err != nil {
			// if 403 or network error retry from a different namenode
			if httputil.IsForbidden(err) || httputil.IsNetworkError(err) {
				log.Infof("HDFS upload error: %s, retrying from the next name node", err)
				continue
			}

			return fmt.Errorf("could not put content to a server: %s", err)
		}

		// follow redirect's location manually per WebHDFS protocol
		loc, ok := nameresp.Header["Location"]
		if !ok || len(loc) == 0 {
			return fmt.Errorf("missing location field in response header: %s",
				nameresp.Header)
		}

		dataresp, err := httputil.Put(
			loc[0],
			httputil.SendBody(r),
			httputil.SendAcceptedCodes(http.StatusCreated),
		)

		defer dataresp.Body.Close()

		if err != nil {
			// if 403 or network error retry from a different namenode
			if httputil.IsForbidden(err) || httputil.IsNetworkError(err) {
				log.Infof("HDFS upload error: %s, retrying from the next name node", err)
				continue
			}

			return fmt.Errorf("could not put content to hdfs server %s", err)
		}

		return nil
	}
	return errAllNameNodesUnavailable
}

// DownloadFile downloads a file from HDFS and writes the data to dst.
func (c *Client) DownloadFile(name string, dst fileio.Writer) error {

	v := c.createParams(HdfsOpen)

	// Note the url needs to have placeholders for a blob name
	// first 2 characters of a blob name (docker registry) and hdfs open parameters,
	// i.e http://addr:port/path/%s/%s/data?%s to be decoded as
	// http://addr:port/path/00/004a3d44c...34/data?%s
	u := fmt.Sprintf("/webhdfs/v1/infra/dockerRegistry/docker/registry/v2/blobs/sha256/%s/%s/data?%s",
		name[:2], name, v.Encode())

	log.Infof("Starting HDFS download from remote backend: %s, name: %s", u, name)
	r, nb, err := c.download(u)
	if err != nil {
		return err
	}

	written, err := io.Copy(dst, r)
	if err != nil {
		return fmt.Errorf("copy response: %s", err)
	}
	if written != nb {
		return fmt.Errorf("content len %d does not match data transfer amount %d", nb, written)
	}
	return nil
}

// DownloadBytes TODO(codyg): Implement.
func (c *Client) DownloadBytes(name string) ([]byte, error) {
	return nil, errors.New("unimplemented")
}

// UploadFile uploads src to HDFS.
func (c *Client) UploadFile(name string, src fileio.Reader) error {
	v := c.createParams(HdfsCreate)

	u := fmt.Sprintf("/webhdfs/v1/infra/dockerRegistry/docker/registry/v2/blobs/sha256/%s/%s/data?%s",
		name[:2], name, v.Encode())

	log.Infof("Starting HDFS upload to remote backend: %s", u)

	return c.upload(u, src)
}

// UploadBytes TODO(codyg): Implement.
func (c *Client) UploadBytes(name string, b []byte) error {
	return errors.New("unimplemented")
}

// GetManifest gets manifest from http backend
func (c *Client) GetManifest(repo, tag string) (io.ReadCloser, error) {

	v := c.createParams(HdfsOpen)
	u := fmt.Sprintf(
		"/webhdfs/v1/infra/dockerRegistry/docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link?%s",
		repo,
		tag,
		v.Encode(),
	)

	log.Infof("Starting docker manifest download from HDFS backend: %s", u)

	r, _, err := c.download(u)

	if err != nil {
		return nil, err
	}
	defer r.Close()

	body, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	d, err := core.NewDigestFromString(string(body))
	if err != nil {
		return nil, err
	}

	// now we have sha256 of a docker manifest, get it in a second call
	bu := fmt.Sprintf("/webhdfs/v1/infra/dockerRegistry/docker/registry/v2/blobs/sha256/%s/%s/data?%s",
		d.Hex()[:2], d.Hex(), v.Encode())

	log.Infof("HDFS downloads manifest from hdfs backend: %s, name: %s", u, d.Hex())
	reader, _, err := c.download(bu)
	return reader, err

}

// PostManifest posts manifest to http backend
func (c *Client) PostManifest(repo, tag string, manifest io.Reader) error {
	v := c.createParams(HdfsCreate)

	mdata, err := ioutil.ReadAll(manifest)
	if err != nil {
		return fmt.Errorf("read manifest: %s", err)
	}

	d, err := core.NewDigester().FromBytes(mdata)
	if err != nil {
		return fmt.Errorf("compute digest: %s", err)
	}

	// Creates a blob first.
	u := fmt.Sprintf("/webhdfs/v1/infra/dockerRegistry/docker/registry/v2/blobs/sha256/%s/%s/data?%s",
		d.Hex()[:2], d.Hex(), v.Encode())

	log.Infof("Starting docker manifest upload to HDFS backend: %s", u)

	err = c.upload(u, bytes.NewReader(mdata))
	if err != nil {
		return err
	}

	// Then create a link to the blob.
	vl := c.createParams(HdfsCreate)
	ul := fmt.Sprintf(
		"/webhdfs/v1/infra/dockerRegistry/docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link?%s",
		repo,
		tag,
		vl.Encode(),
	)

	return c.upload(ul, bytes.NewBufferString(d.String()))
}
