package manifestclient

import (
	"fmt"
	"io"
	"net/url"

	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// Client defines an interface to get and post manifest
type Client interface {
	GetManifest(repo, tag string) (io.ReadCloser, error)
	PostManifest(repo, tag string, manifest io.Reader) error
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
	it := m.servers.Iter()
	for it.Next() {
		resp, err := httputil.Get(fmt.Sprintf("http://%s/manifest/%s", it.Addr(), url.QueryEscape(name)))
		if err != nil {
			if httputil.IsNetworkError(err) {
				log.Errorf("Error getting manifest from %s: %s", it.Addr(), err)
				continue
			}
			return nil, err
		}
		return resp.Body, nil
	}
	return nil, it.Err()
}

// PostManifest posts manifest for repo:tag.
func (m *client) PostManifest(repo, tag string, manifest io.Reader) error {
	name := fmt.Sprintf("%s:%s", repo, tag)
	var err error
	it := m.servers.Iter()
	for it.Next() {
		_, err = httputil.Post(
			fmt.Sprintf("http://%s/manifest/%s", it.Addr(), url.QueryEscape(name)),
			httputil.SendBody(manifest))
		if httputil.IsNetworkError(err) {
			log.Errorf("Error getting manifest from %s: %s", it.Addr(), err)
			continue
		}
		return err
	}
	return it.Err()
}
