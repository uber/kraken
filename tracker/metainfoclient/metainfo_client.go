package metainfoclient

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client errors.
var (
	ErrExists   = errors.New("metainfo already exists")
	ErrNotFound = errors.New("metainfo not found")
)

// Client defines operations on torrent metainfo.
type Client interface {
	Download(name string) (*torlib.MetaInfo, error)
	Upload(mi *torlib.MetaInfo) error
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
// no torrent exists under name.
func (c *client) Download(name string) (*torlib.MetaInfo, error) {
	var err error
	for it := c.servers.Iter(); it.HasNext(); it.Next() {
		var resp *http.Response
		resp, err = httputil.Get(
			fmt.Sprintf("http://%s/info?name=%s", it.Addr(), name),
			httputil.SendTimeout(c.config.Timeout))
		if err != nil {
			if httputil.IsNotFound(err) {
				return nil, ErrNotFound
			}
			continue
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

// Upload uploads mi to storage. Returns ErrExists if there is a name conflict
// for mi.
func (c *client) Upload(mi *torlib.MetaInfo) error {
	s, err := mi.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing metainfo: %s", err)
	}
	for it := c.servers.Iter(); it.HasNext(); it.Next() {
		_, err = httputil.Post(
			fmt.Sprintf("http://%s/info", it.Addr()),
			httputil.SendBody(bytes.NewBufferString(s)),
			httputil.SendTimeout(c.config.Timeout))
		if err != nil {
			if httputil.IsConflict(err) {
				return ErrExists
			}
			continue
		}
		return nil
	}
	return err
}
