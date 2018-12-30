package security

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/uber/kraken/utils/httputil"

	"github.com/docker/docker-credential-helpers/client"
	"github.com/docker/engine-api/types"
)

const tokenUsername = "<token>"

var credentialHelperPrefix = "docker-credential-"

// Config contains tls and basic auth configuration.
type Config struct {
	TLS                    *httputil.TLSConfig `yaml:"tls"`
	BasicAuth              *types.AuthConfig   `yaml:"basic"`
	RemoteCredentialsStore string              `yaml:"credsStore"`
}

// GetHTTPOption returns httputil.Option based on the security configuration.
func (c Config) GetHTTPOption(addr, repo string) (httputil.SendOption, error) {
	shouldUseBasicAuth := (c.BasicAuth != nil || c.RemoteCredentialsStore != "")

	var tlsClientConfig *tls.Config
	var err error
	if c.TLS != nil {
		tlsClientConfig, err = c.TLS.BuildClient()
		if err != nil {
			return nil, fmt.Errorf("build tls config: %s", err)
		}
		if !shouldUseBasicAuth {
			return httputil.SendTLS(tlsClientConfig), nil
		}
	}

	if shouldUseBasicAuth {
		authConfig, err := c.getCredentials(c.RemoteCredentialsStore, addr)
		if err != nil {
			return nil, fmt.Errorf("get credentials: %s", err)
		}
		tr := http.DefaultTransport.(*http.Transport)
		tr.TLSClientConfig = tlsClientConfig // If tlsClientConfig is nil, default is used.
		rt, err := BasicAuthTransport(addr, repo, tr, authConfig)
		if err != nil {
			return nil, fmt.Errorf("basic auth: %s", err)
		}
		return httputil.SendTLSTransport(rt), nil
	}
	return httputil.SendNoop(), nil
}

func (c Config) getCredentials(helper, addr string) (types.AuthConfig, error) {
	var authConfig types.AuthConfig
	if c.BasicAuth != nil {
		authConfig = *c.BasicAuth
	}
	var err error
	if helper != "" {
		authConfig, err = c.getCredentialFromHelper(helper, addr)
		if err != nil {
			return types.AuthConfig{}, fmt.Errorf("get credentials from helper %s: %s", helper, err)
		}
	}
	return authConfig, nil
}

func (c Config) getCredentialFromHelper(helper, addr string) (types.AuthConfig, error) {
	helperFullName := credentialHelperPrefix + helper
	creds, err := client.Get(client.NewShellProgramFunc(helperFullName), addr)
	if err != nil {
		return types.AuthConfig{}, err
	}

	var ret types.AuthConfig
	if c.BasicAuth != nil {
		ret = *c.BasicAuth
	}
	ret.ServerAddress = addr
	if creds.Username == tokenUsername {
		ret.IdentityToken = creds.Secret
	} else {
		ret.Password = creds.Secret
		ret.Username = creds.Username
	}
	return ret, nil
}
