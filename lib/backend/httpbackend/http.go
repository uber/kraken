package httpbackend

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
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

// DownloadFile downloads the content from a configured url and writes the data
// to dst.
func (c *Client) DownloadFile(name string, dst fileio.Writer) error {
	b := new(bytes.Buffer)

	// using Fprintf instead of Sprintf to handle formatting errors
	_, err := fmt.Fprintf(b, c.config.DownloadURL, name)
	if err != nil {
		return fmt.Errorf("could not create a url: %s", err)
	}
	log.Infof("Starting HTTP download from remote backend: %s", b.String())

	resp, err := httputil.Get(b.String())
	if err != nil {
		if httputil.IsNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
		return fmt.Errorf("could not get a content from http backend: %s", err)
	}

	defer resp.Body.Close()

	_, err = io.Copy(dst, resp.Body)
	if err != nil {
		return fmt.Errorf("could not copy response buffer: %s", err)
	}

	return err
}

// DownloadBytes TODO(codyg): Implement.
func (c *Client) DownloadBytes(name string) ([]byte, error) {
	return nil, errors.New("unimplemented")
}

// UploadFile TODO(codyg): Implement.
func (c *Client) UploadFile(name string, src fileio.Reader) error {
	return errors.New("unimplemented")
}

// UploadBytes TODO(codyg): Implement.
func (c *Client) UploadBytes(name string, b []byte) error {
	return errors.New("unimplemented")
}
