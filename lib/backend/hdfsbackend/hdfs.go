package hdfsbackend

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"code.uber.internal/infra/kraken/lib/fileio"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config manages HDFS connection parameters,
// host:port, authentication info,
// token, etc
type Config struct {
	NameNodeURL string `yaml:"name_node"` // host
	BuffSize    int64  `yaml:"buff_size"` // default transfer block size
	UserName    string `yaml:"username"`  // auth username
	RootPath    string `yaml:"root_path"` // rootpath to read all files at at this system
}

const (
	// HDFS WebDAV operations, there will be more
	// currently we only support file reads

	// HdfsOpen defines open operator
	HdfsOpen = "open"

	// HdfsCreate defines create operator
	HdfsCreate = "create"
)

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
func NewHDFSClient(config Config) *Client {
	return &Client{config: config.applyDefaults()}
}

// Download downloads a file from HDFS datastore, writes it
// into input writer
func (c *Client) Download(name string, dst fileio.Writer) (int64, error) {

	v := url.Values{}

	if c.config.UserName != "" {
		v.Set("user.name", c.config.UserName)
	}

	v.Set("buffersize", strconv.FormatInt(c.config.BuffSize, 10))
	v.Set("op", HdfsOpen)
	u := fmt.Sprintf("%s/%s?%s", c.config.NameNodeURL, name, v.Encode())

	log.Infof("Starting HDFS download from remote backend: %s", u)

	resp, err := httputil.Get(u)
	if err != nil {
		return 0, fmt.Errorf("could not get a content from hdfs: %s", err)
	}

	defer resp.Body.Close()

	written, err := io.Copy(dst, resp.Body)
	if err != nil {
		return 0, fmt.Errorf("could not copy response buffer: %s", err)
	}

	if written != resp.ContentLength {
		return 0, fmt.Errorf("content len %d does not match data transfer amount %d", resp.ContentLength, written)
	}

	return written, nil
}

// Upload reads bytes from input reader pushing file to a remote
// HDFS store
func (c *Client) Upload(name string, src fileio.Reader) error {

	v := url.Values{}

	if c.config.UserName != "" {
		v.Set("user.name", c.config.UserName)
	}

	v.Set("buffersize", strconv.FormatInt(c.config.BuffSize, 10))
	v.Set("overwrite", "true")
	v.Set("op", HdfsCreate)
	u := fmt.Sprintf("%s/%s?%s", c.config.NameNodeURL, name, v.Encode())

	log.Infof("Starting HDFS upload to remote backend: %s", u)

	// WebHDFS protocols requires that create file request should be
	// sent without being automatically following a redirect
	nameresp, err := httputil.Put(
		u,
		httputil.SendRedirect(
			func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		),
		httputil.SendAcceptedCodes(http.StatusTemporaryRedirect,
			http.StatusPermanentRedirect),
	)

	if err != nil {
		return fmt.Errorf("could not put content to a server: %s", err)
	}
	defer nameresp.Body.Close()

	// follow redirect's location manually per WebHDFS protocol
	loc, ok := nameresp.Header["Location"]
	if !ok || len(loc) == 0 {
		return fmt.Errorf("missing location field in response header: %s",
			nameresp.Header)
	}

	dataresp, err := httputil.Put(
		loc[0],
		httputil.SendBody(src),
		httputil.SendAcceptedCodes(http.StatusCreated),
	)

	if err != nil {
		return fmt.Errorf("could not put content to hdfs server %s", err)
	}

	defer dataresp.Body.Close()

	return nil
}
