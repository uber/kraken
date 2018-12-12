package lib

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"

	"github.com/uber/kraken/utils/httputil"

	"gopkg.in/yaml.v2"
)

// ReadTLSFile reads config file in path and returns *tls.Config.
// It returns nil when path is nil.
func ReadTLSFile(path *string) (*tls.Config, error) {
	if path == nil {
		return nil, nil
	}
	data, err := ioutil.ReadFile(*path)
	if err != nil {
		return nil, fmt.Errorf("read tls config: %s", err)
	}
	var config httputil.TLSConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unmarshal tls config: %s", err)
	}
	tls, err := config.BuildClient()
	if err != nil {
		return nil, fmt.Errorf("build tls client: %s", err)
	}
	return tls, nil
}
