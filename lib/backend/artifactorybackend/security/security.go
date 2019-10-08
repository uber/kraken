// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package security

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/docker/docker-credential-helpers/client"
	"github.com/docker/engine-api/types"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"
)

const tokenUsername = "<token>"

var credentialHelperPrefix = "docker-credential-"

// Config contains tls and basic auth configuration.
type Config struct {
	TLS                    httputil.TLSConfig `yaml:"tls"`
	BasicAuth              *types.AuthConfig  `yaml:"basic"`
	RemoteCredentialsStore string             `yaml:"credsStore"`
}

// RegistryToken is a bearer token to be sent to a registry
type registryToken struct {
	mux sync.Mutex
	tokenHeader map[string]string
	startTime time.Time
	// validInterval in ns
	validInterval int64
}

var regToken = registryToken{tokenHeader: make(map[string]string)}

const tokenQuery = "http://%s/v2/token"

type tokenAPIResponse struct {
	Token string `json:"token"`
	Expire uint32 `json:"expires_in"`
}

// GetHTTPOption returns httputil.Option based on the security configuration.
func (c Config) GetHTTPOption(addr, repo string) (httputil.SendOption, error) {
	if c.TLS.Client.Disabled {
		return httputil.SendNoop(), nil
	}

	shouldUseBasicAuth := (c.BasicAuth != nil || c.RemoteCredentialsStore != "")
	tlsClientConfig, err := c.TLS.BuildClient()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %s", err)
	}
	if !shouldUseBasicAuth {
		return httputil.SendTLS(tlsClientConfig), nil
	}

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

func GetAuthHeader(address string, opt httputil.SendOption) (map[string]string, error) {
	URL := fmt.Sprintf(tokenQuery, address)

	regToken.mux.Lock()
	defer regToken.mux.Unlock()
	currentTime := time.Now()
	if len(regToken.tokenHeader) == 0 || (currentTime.Sub(regToken.startTime).Nanoseconds() >= regToken.validInterval) {
		// Need to obtain a new token
		resp, err := httputil.Get(
			URL,
			opt)
		if err != nil {
			return nil, fmt.Errorf("get token: %s", err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read token response: %s", err)
		}

		token := tokenAPIResponse{}
		err = json.Unmarshal(body, &token)
		if err != nil {
			return nil, fmt.Errorf("unmarshal token API response: %s", err)
		}
		regToken.tokenHeader["Authorization"] = token.Token
		regToken.startTime = currentTime
		regToken.validInterval = int64(token.Expire) * 1e+9
		log.Infof("Update regToken %v", regToken)
	}
	// Deep copy to avoid race condition
	res := make(map[string]string)
	for k, v := range regToken.tokenHeader {
		res[k] = v
	}
	return res, nil
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
