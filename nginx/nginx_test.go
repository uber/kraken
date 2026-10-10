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
	t.Helper()
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

	tests := []struct {
		desc       string
		cas        []string
		cert       string
		key        string
		passphrase string
		wantErr    string
	}{
		{
			desc: "no passphrase",
			cas:  []string{ca},
			cert: cert,
			key:  key,
		}, {
			desc:       "with passphrase",
			cas:        []string{ca},
			cert:       cert,
			key:        key,
			passphrase: passphrase,
		}, {
			desc:       "missing passphrase file",
			cas:        []string{ca},
			cert:       cert,
			key:        key,
			passphrase: missing,
			wantErr:    "tls.server.passphrase.path",
		}, {
			desc:    "missing cert file",
			cas:     []string{ca},
			cert:    missing,
			key:     key,
			wantErr: "tls.server.cert.path",
		}, {
			desc:    "empty key path",
			cas:     []string{ca},
			cert:    cert,
			key:     "",
			wantErr: "tls.server.key.path is required",
		}, {
			desc:    "missing ca file",
			cas:     []string{ca, missing},
			cert:    cert,
			key:     key,
			wantErr: "tls.cas[1].path",
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			c := httputil.TLSConfig{
				Server: httputil.X509Pair{
					Cert:       httputil.Secret{Path: test.cert},
					Key:        httputil.Secret{Path: test.key},
					Passphrase: httputil.Secret{Path: test.passphrase},
				},
			}
			for _, p := range test.cas {
				c.CAs = append(c.CAs, httputil.Secret{Path: p})
			}
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
