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
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/awslabs/amazon-ecr-credential-helper/ecr-login"
	"github.com/awslabs/amazon-ecr-credential-helper/ecr-login/api"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"

	"github.com/docker/distribution/registry/client/auth"
	"github.com/docker/distribution/registry/client/auth/challenge"
	"github.com/docker/distribution/registry/client/transport"
	"github.com/docker/docker-credential-helpers/client"
	"github.com/docker/engine-api/types"
)

const (
	basePingQuery          = "http://%s/v2/"
	registryVersionHeader  = "Docker-Distribution-Api-Version"
	tokenUsername          = "<token>"
	credentialHelperPrefix = "docker-credential-"
)

var v2Version = auth.APIVersion{
	Type:    "registry",
	Version: "2.0",
}

// Config contains tls and basic auth configuration.
type Config struct {
	TLS                    httputil.TLSConfig `yaml:"tls"`
	BasicAuth              *types.AuthConfig  `yaml:"basic"`
	RemoteCredentialsStore string             `yaml:"credsStore"`
	EnableHTTPFallback     bool               `yaml:"enableHTTPFallback"`
}

// Authenticator creates send options to authenticate requests to registry
// backends.
type Authenticator interface {
	// Authenticate returns a send option to authenticate to the registry,
	// scoped to the given image repository.
	Authenticate(repo string) ([]httputil.SendOption, error)
}

type authenticator struct {
	address          string
	config           Config
	roundTripper     http.RoundTripper
	credentialStore  auth.CredentialStore
	challengeManager challenge.Manager
	tokenHandlers    sync.Map
}

// NewAuthenticator returns a new authenticator for the given docker registry
// address, TLS, and credentials configuration. It supports both basic auth and
// token based authentication challenges. If TLS is disabled, no authentication
// is attempted.
func NewAuthenticator(address string, config Config, transport *http.Transport) (Authenticator, error) {
	tlsClientConfig, err := config.TLS.BuildClient()
	if err != nil {
		return nil, fmt.Errorf("build tls config for %q: %s", address, err)
	}
	transport.TLSClientConfig = tlsClientConfig
	return &authenticator{
		address:          address,
		config:           config,
		roundTripper:     transport,
		credentialStore:  newCredentialStore(address, config),
		challengeManager: challenge.NewSimpleManager(),
	}, nil
}

func (a *authenticator) Authenticate(repo string) ([]httputil.SendOption, error) {
	config := a.config

	var opts []httputil.SendOption
	if config.TLS.Client.Disabled {
		opts = append(opts, httputil.SendNoop())
		return opts, nil
	}

	if !config.EnableHTTPFallback {
		opts = append(opts, httputil.DisableHTTPFallback())
	}
	if !a.shouldAuth() {
		opts = append(opts, httputil.SendTLSTransport(a.roundTripper))
		return opts, nil
	}
	if err := a.updateChallenge(); err != nil {
		return nil, fmt.Errorf("could not update auth challenge: %s", err)
	}
	opts = append(opts, httputil.SendTLSTransport(a.transport(repo)))
	return opts, nil
}

func (a *authenticator) shouldAuth() bool {
	return a.config.BasicAuth != nil || a.config.RemoteCredentialsStore != ""
}

func (a *authenticator) transport(repo string) http.RoundTripper {
	basicHandler := auth.NewBasicHandler(a.credentialStore)
	bearerHandler, _ := a.tokenHandlers.LoadOrStore(repo, auth.NewTokenHandlerWithOptions(auth.TokenHandlerOptions{
		Transport:   a.roundTripper,
		Credentials: a.credentialStore,
		Scopes: []auth.Scope{
			auth.RepositoryScope{
				Repository: repo,
				Actions:    []string{"pull", "push"},
			},
		},
		ClientID: "docker",
	}))
	return transport.NewTransport(a.roundTripper, auth.NewAuthorizer(a.challengeManager, basicHandler, bearerHandler.(auth.AuthenticationHandler)))
}

func (a *authenticator) updateChallenge() error {
	resp, err := httputil.Send(
		"GET",
		fmt.Sprintf(basePingQuery, a.address),
		httputil.SendTLSTransport(a.roundTripper),
		httputil.SendAcceptedCodes(http.StatusOK, http.StatusUnauthorized),
	)
	if err != nil {
		return err
	}
	versions := auth.APIVersions(resp, registryVersionHeader)
	for _, version := range versions {
		if version == v2Version {
			if err := a.challengeManager.AddResponse(resp); err != nil {
				return fmt.Errorf("add response: %s", err)
			}
			return nil
		}
	}
	return fmt.Errorf("registry is not v2")
}

type credentialStore struct {
	address string
	config  Config
}

func newCredentialStore(address string, config Config) *credentialStore {
	return &credentialStore{
		address: address,
		config:  config,
	}
}

func (c credentialStore) Basic(*url.URL) (string, string) {
	if username, password := c.credentialsFromHelper(); username != "" && username != tokenUsername {
		return username, password
	}
	basic := c.config.BasicAuth
	if basic == nil {
		return "", ""
	}
	return basic.Username, basic.Password
}

func (c credentialStore) RefreshToken(*url.URL, string) string {
	if username, token := c.credentialsFromHelper(); username == tokenUsername {
		return token
	}
	basic := c.config.BasicAuth
	if basic == nil {
		return ""
	}
	return basic.IdentityToken
}

func (c credentialStore) credentialsFromHelper() (string, string) {
	switch c.config.RemoteCredentialsStore {
	case "":
		// No credential helper configured, caller will use static credentials
		// from configuration.
		return "", ""
	case "ecr-login":
		client := ecr.ECRHelper{ClientFactory: api.DefaultClientFactory{}}
		username, password, err := client.Get(c.address)
		if err != nil {
			log.Errorf("get credentials from helper ECR for %q: %s", c.address, err)
		}
		return username, password
	default:
		helper := credentialHelperPrefix + c.config.RemoteCredentialsStore
		creds, err := client.Get(client.NewShellProgramFunc(helper), c.address)
		if err != nil {
			log.Errorf("get credentials from helper %s for %q: %s", c.config.RemoteCredentialsStore, c.address, err)
			return "", ""
		}
		return creds.Username, creds.Secret
	}
}

func (c credentialStore) SetRefreshToken(*url.URL, string, string) {}
