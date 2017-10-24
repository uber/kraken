package metainfoclient

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client defines operations on torrent metainfo.
type Client interface {
	Get(name string) (*torlib.MetaInfo, error)
	Post(mi *torlib.MetaInfo) error
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

// Get returns the MetaInfo associated with name.
func (c *client) Get(name string) (*torlib.MetaInfo, error) {
	var err error
	for it := c.servers.Iter(); it.HasNext(); it.Next() {
		var resp *http.Response
		resp, err = httputil.Get(
			fmt.Sprintf("http://%s/info?name=%s", it.Addr(), name),
			httputil.SendTimeout(c.config.Timeout))
		if err != nil {
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading body: %s", err)
		}
		mi, err := torlib.NewMetaInfoFromBytes(b)
		if err != nil {
			return nil, fmt.Errorf("error parsing metainfo: %s", err)
		}
		return mi, nil
	}
	return nil, err
}

// Post writes mi to storage.
func (c *client) Post(mi *torlib.MetaInfo) error {
	s, err := mi.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing metainfo: %s", err)
	}
	for it := c.servers.Iter(); it.HasNext(); it.Next() {
		_, err = httputil.Post(
			fmt.Sprintf("http://%s/info?name=%s&info_hash=%s", it.Addr(), mi.Name(), mi.InfoHash.HexString()),
			httputil.SendBody(bytes.NewBufferString(s)),
			httputil.SendTimeout(c.config.Timeout))
		if err != nil {
			continue
		}
		return nil
	}
	return err
}
