package metainfoclient

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client errors.
var (
	ErrExists   = errors.New("metainfo already exists")
	ErrNotFound = errors.New("metainfo not found")
	ErrRetry    = errors.New("request accepted, retry later")
)

// Client defines operations on torrent metainfo.
type Client interface {
	Download(namespace string, name string) (*torlib.MetaInfo, error)
}

type client struct {
	config  Config
	servers serverset.Set
}

// New returns a new Client.
func New(config Config, servers serverset.Set) Client {
	return &client{config.applyDefaults(), servers}
}

// Default returns a default Client.
func Default(servers serverset.Set) Client {
	return New(Config{}, servers)
}

// Download returns the MetaInfo associated with name. Returns ErrNotFound if
// no torrent exists under name, or ErrRetry if the metainfo is still generating.
func (c *client) Download(namespace string, name string) (*torlib.MetaInfo, error) {
	d := image.NewSHA256DigestFromHex(name)
	var err error
	for it := c.servers.Iter(); it.HasNext(); it.Next() {
		var resp *http.Response
		resp, err = httputil.Get(
			fmt.Sprintf("http://%s/namespace/%s/blobs/%s/metainfo", it.Addr(), namespace, d),
			httputil.SendTimeout(c.config.Timeout))
		if err != nil {
			switch v := err.(type) {
			case httputil.NetworkError:
				continue
			case httputil.StatusError:
				switch v.Status {
				case http.StatusAccepted:
					err = ErrRetry
				case http.StatusNotFound:
					err = ErrNotFound
				}
			}
			return nil, err
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading body: %s", err)
		}
		mi, err := torlib.DeserializeMetaInfo(b)
		if err != nil {
			return nil, fmt.Errorf("error parsing metainfo: %s", err)
		}
		return mi, nil
	}
	return nil, err
}
