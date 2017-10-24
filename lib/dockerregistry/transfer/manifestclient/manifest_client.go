package manifestclient

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client defines an interface to get and post manifest
type Client interface {
	GetManifest(repo, tag string) (io.ReadCloser, error)
	PostManifest(repo, tag, digest string, manifest io.Reader) error
}

type client struct {
	servers serverset.Set
}

// New creates a new Client.
func New(servers serverset.Set) Client {
	return &client{servers}
}

// GetManifest gets the manifest of repo:tag.
func (m *client) GetManifest(repo, tag string) (io.ReadCloser, error) {
	name := fmt.Sprintf("%s:%s", repo, tag)
	var err error
	for it := m.servers.Iter(); it.HasNext(); it.Next() {
		var resp *http.Response
		resp, err = httputil.Get(fmt.Sprintf("http://%s/manifest/%s", it.Addr(), url.QueryEscape(name)))
		if err == nil {
			return resp.Body, nil
		}
	}
	return nil, err
}

// PostManifest posts manifest for repo:tag.
func (m *client) PostManifest(repo, tag, digest string, manifest io.Reader) error {
	name := fmt.Sprintf("%s:%s", repo, tag)
	var err error
	for it := m.servers.Iter(); it.HasNext(); it.Next() {
		_, err = httputil.Post(
			fmt.Sprintf("http://%s/manifest/%s", it.Addr(), url.QueryEscape(name)),
			httputil.SendBody(manifest))
		if err == nil {
			return nil
		}
	}
	return err
}
