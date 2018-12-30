package registrybackend

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/httputil"
	yaml "gopkg.in/yaml.v2"
)

const _registryblob = "registry_blob"

func init() {
	backend.Register(_registryblob, &blobClientFactory{})
}

type blobClientFactory struct{}

func (f *blobClientFactory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal hdfs config")
	}
	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal hdfs config")
	}
	return NewBlobClient(config)
}

const _blobquery = "http://%s/v2/%s/blobs/%s"

// BlobClient stats and downloads blob from registry.
type BlobClient struct {
	config Config
}

// NewBlobClient creates a new BlobClient.
func NewBlobClient(config Config) (*BlobClient, error) {
	return &BlobClient{config}, nil
}

// Stat sends a HEAD request to registry for a blob and returns the blob size.
func (c *BlobClient) Stat(namespace, name string) (*core.BlobInfo, error) {
	opt, err := c.config.Security.GetHTTPOption(c.config.Address, namespace)
	if err != nil {
		return nil, fmt.Errorf("get security opt: %s", err)
	}

	URL := fmt.Sprintf(_blobquery, c.config.Address, namespace, name)
	resp, err := httputil.Head(
		URL,
		opt,
		httputil.SendAcceptedCodes(http.StatusOK, http.StatusNotFound))
	if err != nil {
		return nil, fmt.Errorf("check blob exists: %s", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, backenderrors.ErrBlobNotFound
	}
	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse blob size: %s", err)
	}
	return core.NewBlobInfo(size), nil
}

// Download gets a blob from registry.
func (c *BlobClient) Download(namespace, name string, dst io.Writer) error {
	opt, err := c.config.Security.GetHTTPOption(c.config.Address, namespace)
	if err != nil {
		return fmt.Errorf("get security opt: %s", err)
	}

	URL := fmt.Sprintf(_blobquery, c.config.Address, namespace, name)
	resp, err := httputil.Get(
		URL,
		opt,
		httputil.SendAcceptedCodes(http.StatusOK, http.StatusNotFound))
	if err != nil {
		return fmt.Errorf("check blob exists: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return backenderrors.ErrBlobNotFound
	}
	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	return nil
}

// Upload is not supported as users can push directly to registry.
func (c *BlobClient) Upload(namespace, name string, src io.Reader) error {
	return errors.New("not supported")
}

// List is not supported for blobs.
func (c *BlobClient) List(prefix string) ([]string, error) {
	return nil, errors.New("not supported")
}
