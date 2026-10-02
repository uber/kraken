// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package nginx

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/httputil"
)

func writeTLSFiles(t *testing.T) (ca, cert, key, passphrase string) {
	dir := t.TempDir()
	for _, name := range []string{"ca.crt", "tls.crt", "tls.key", "passphrase"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600))
	}
	return filepath.Join(dir, "ca.crt"),
		filepath.Join(dir, "tls.crt"),
		filepath.Join(dir, "tls.key"),
		filepath.Join(dir, "passphrase")
}

func TestValidateTLSFiles(t *testing.T) {
	ca, cert, key, passphrase := writeTLSFiles(t)
	missing := filepath.Join(t.TempDir(), "missing")

	newConfig := func() httputil.TLSConfig {
		return httputil.TLSConfig{
			CAs: []httputil.Secret{{Path: ca}},
			Server: httputil.X509Pair{
				Cert: httputil.Secret{Path: cert},
				Key:  httputil.Secret{Path: key},
			},
		}
	}

	tests := []struct {
		desc    string
		modify  func(*httputil.TLSConfig)
		wantErr string
	}{
		{"no passphrase", func(*httputil.TLSConfig) {}, ""},
		{"with passphrase", func(c *httputil.TLSConfig) {
			c.Server.Passphrase.Path = passphrase
		}, ""},
		{"missing passphrase file", func(c *httputil.TLSConfig) {
			c.Server.Passphrase.Path = missing
		}, "tls.server.passphrase.path"},
		{"missing cert file", func(c *httputil.TLSConfig) {
			c.Server.Cert.Path = missing
		}, "tls.server.cert.path"},
		{"empty key path", func(c *httputil.TLSConfig) {
			c.Server.Key.Path = ""
		}, "tls.server.key.path is required"},
		{"missing ca file", func(c *httputil.TLSConfig) {
			c.CAs = append(c.CAs, httputil.Secret{Path: missing})
		}, "tls.cas[1].path"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			c := newConfig()
			test.modify(&c)
			err := validateTLSFiles(c)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.wantErr)
			}
		})
	}
}

func TestBuildPasswordFile(t *testing.T) {
	ca, cert, key, passphrase := writeTLSFiles(t)

	site := filepath.Join(t.TempDir(), "site")
	require.NoError(t, os.WriteFile(site, []byte("server {}"), 0o600))

	build := func(passphrasePath string) string {
		c := Config{TemplatePath: site}
		WithTLS(httputil.TLSConfig{
			CAs: []httputil.Secret{{Path: ca}},
			Server: httputil.X509Pair{
				Cert:       httputil.Secret{Path: cert},
				Key:        httputil.Secret{Path: key},
				Passphrase: httputil.Secret{Path: passphrasePath},
			},
		})(&c)
		src, err := c.Build(map[string]interface{}{})
		require.NoError(t, err)
		return string(src)
	}

	require.NotContains(t, build(""), "ssl_password_file")
	require.Contains(t, build(passphrase), "ssl_password_file "+passphrase+";")
}
