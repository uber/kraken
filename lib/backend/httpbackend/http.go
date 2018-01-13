package httpbackend

import (
	"bytes"
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/fileio"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// Config defines http post/get upload/download urls
// and http connnection parameters. The URLs come with string format
// specifiers and define how to pass sha256 parameters
type Config struct {
	UploadURL   string `yaml:"upload_url"`   // http upload post url
	DownloadURL string `yaml:"download_url"` // http download get url
}

// Client implements downloading/uploading object from/to S3
type Client struct {
	config Config
}

func (c Config) applyDefaults() Config {
	return c
}

// NewClient creates s3 client from input parameters
func NewClient(config Config) (*Client, error) {
	config = config.applyDefaults()
	return &Client{config: config}, nil
}

// Download downloads the content from a pre-configured input url
// into provided writer
func (c *Client) Download(name string, dst fileio.Writer) error {
	b := new(bytes.Buffer)

	// using Fprintf instead of Sprintf to handle formatting errors
	_, err := fmt.Fprintf(b, c.config.DownloadURL, name)
	if err != nil {
		return fmt.Errorf("could not create a url: %s", err)
	}
	log.Infof("Starting HTTP download from remote backend: %s", b.String())

	resp, err := httputil.Get(b.String())
	if err != nil {
		return fmt.Errorf("could not get a content from http backend: %s", err)
	}

	defer resp.Body.Close()

	_, err = io.Copy(dst, resp.Body)
	if err != nil {
		return fmt.Errorf("could not copy response buffer: %s", err)
	}

	return err
}

// Upload uploads the content for a given input bucket and key reading
// data from a provided reader
func (c *Client) Upload(name string, src fileio.Reader) error {
	return fmt.Errorf("we don't support upload for http backends for now")
}
