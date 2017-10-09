package transfer

import (
	"fmt"
	"io"
	"net/url"

	"code.uber.internal/infra/kraken/utils/httputil"
)

// ManifestClient defines an interface to get and post manifest
type ManifestClient interface {
	GetManifest(repo, tag string) (io.ReadCloser, error)
	PostManifest(repo, tag, manifest string, reader io.Reader) error
}

// HTTPManifestClient gets and posts manifest via http
type HTTPManifestClient struct {
	addr string
}

// GetManifest gets manfiest given address, repo and tag
func (m *HTTPManifestClient) GetManifest(repo, tag string) (io.ReadCloser, error) {
	name := fmt.Sprintf("%s:%s", repo, tag)
	url := "http://" + m.addr + "/manifest/" + url.QueryEscape(name)

	resp, err := httputil.Send("GET", url)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to %s: %s", url, err)
	}
	return resp.Body, nil
}

// PostManifest posts manifest to a given addr
func (m *HTTPManifestClient) PostManifest(repo, tag, manifest string, reader io.Reader) error {
	name := fmt.Sprintf("%s:%s", repo, tag)
	url := "http://" + m.addr + "/manifest/" + url.QueryEscape(name)
	resp, err := httputil.Send("POST", url, httputil.SendBody(reader))
	if err != nil {
		return fmt.Errorf("failed to send post request to %s: %s", url, err)
	}
	defer resp.Body.Close()

	return nil
}
