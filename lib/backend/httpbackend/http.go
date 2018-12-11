package httpbackend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/httputil"

	"gopkg.in/yaml.v2"
)

const _http = "http"

func init() {
	backend.Register(_http, &factory{})
}

type factory struct{}

func (f *factory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal http config")
	}

	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal http config")
	}
	return NewClient(config)
}

// Config defines http post/get upload/download urls
// and http connnection parameters. The URLs come with string format
// specifiers and define how to pass sha256 parameters
type Config struct {
	UploadURL   string `yaml:"upload_url"`   // http upload post url
	DownloadURL string `yaml:"download_url"` // http download get url
	Timeout     time.Duration
}

// Client implements downloading/uploading object from/to S3
type Client struct {
	config Config
}

func (c Config) applyDefaults() Config {
	if c.Timeout == 0 {
		c.Timeout = 180 * time.Second
	}
	return c
}

// NewClient creates a new http Client.
func NewClient(config Config) (*Client, error) {
	return &Client{config: config.applyDefaults()}, nil
}

// Stat always succeeds.
// TODO(codyg): Support stat URL.
func (c *Client) Stat(name string) (*core.BlobInfo, error) {
	return core.NewBlobInfo(0), nil
}

// Download downloads the content from a configured url and writes the data
// to dst.
func (c *Client) Download(name string, dst io.Writer) error {
	// Use Fprintf instead of Sprintf to handle formatting errors.
	var b bytes.Buffer
	if _, err := fmt.Fprintf(&b, c.config.DownloadURL, name); err != nil {
		return fmt.Errorf("format url: %s", err)
	}
	resp, err := httputil.Get(
		b.String(),
		httputil.SendTimeout(c.config.Timeout),
		httputil.SendRetry())
	if err != nil {
		if httputil.IsNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	return nil
}

// Upload is not supported.
func (c *Client) Upload(name string, src io.Reader) error {
	return errors.New("not supported")
}

// List is not supported.
func (c *Client) List(prefix string) ([]string, error) {
	return nil, errors.New("not supported")
}
