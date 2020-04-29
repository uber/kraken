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
package httputil

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/uber/kraken/utils/log"
)

// ErrEmptyCommonName is returned when common name is not provided for key generation.
var ErrEmptyCommonName = errors.New("empty common name")

// TLSConfig defines TLS configuration.
type TLSConfig struct {
	Name   string   `yaml:"name"`
	Server X509Pair `yaml:"server"`
	Client X509Pair `yaml:"client"`
	CAs    []Secret `yaml:"cas"`

	// Lazy init.
	tls *tls.Config
}

// X509Pair contains x509 cert configuration.
// Both Cert and Key should be already in pem format.
type X509Pair struct {
	Disabled   bool   `yaml:"disabled"`
	Cert       Secret `yaml:"cert"`
	Key        Secret `yaml:"key"`
	Passphrase Secret `yaml:"passphrase"`
}

// Secret contains secret path configuration.
type Secret struct {
	Path string `yaml:"path"`
}

// BuildClient builts tls.Config for http client.
func (c *TLSConfig) BuildClient() (*tls.Config, error) {
	if c.Client.Disabled {
		log.Infof("Client TLS is disabled")
		return nil, nil
	}
	if c.tls != nil {
		return c.tls, nil
	}

	var caPool *x509.CertPool
	var certs []tls.Certificate
	var err error
	if len(c.CAs) > 0 {
		caPool, err = createCertPool(c.CAs)
		if err != nil {
			return nil, fmt.Errorf("create cert pool: %s", err)
		}
	}
	if c.Client.Cert.Path != "" {
		certPEM, err := parseCert(c.Client.Cert.Path)
		if err != nil {
			return nil, fmt.Errorf("parse client cert: %s", err)
		}
		keyPEM, err := parseKey(c.Client.Key.Path, c.Client.Passphrase.Path)
		if err != nil {
			return nil, fmt.Errorf("parse client key: %s", err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("load client x509 key pair: %s", err)
		}
		certs = []tls.Certificate{cert}
	}
	c.tls = &tls.Config{
		Certificates:             certs,
		RootCAs:                  caPool,
		ServerName:               c.Name,
		PreferServerCipherSuites: true,
		InsecureSkipVerify:       false, // This is important to enforce verification of server.
	}
	return c.tls, nil
}

// WriteCABundle writes a list of CA to a writer.
func (c *TLSConfig) WriteCABundle(w io.Writer) error {
	pems, err := concatSecrets(c.CAs)
	if err != nil {
		return fmt.Errorf("concat secrets: %s", err)
	}
	if _, err := w.Write(pems); err != nil {
		return fmt.Errorf("write cas: %s", err)
	}
	return nil
}

func createCertPool(secrets []Secret) (*x509.CertPool, error) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("create system cert pool: %s", err)
	}
	// No system certs provided. Create an empty cert pool.
	if pool == nil {
		pool = x509.NewCertPool()
	}
	pems, err := concatSecrets(secrets)
	if err != nil {
		return nil, fmt.Errorf("concat secrets: %s", err)
	}
	if ok := pool.AppendCertsFromPEM(pems); !ok {
		return nil, fmt.Errorf("cannot append cert")
	}
	return pool, nil
}

func concatSecrets(secrets []Secret) ([]byte, error) {
	result := bytes.Buffer{}
	for _, s := range secrets {
		pem, err := parseCert(s.Path)
		if err != nil {
			return nil, fmt.Errorf("parse cert: %s", err)
		}
		result.Write(pem)
	}
	return result.Bytes(), nil
}

func parseCert(path string) ([]byte, error) {
	certBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %s", err)
	}
	return certBytes, nil
}

// parseKey reads key from file and decrypts if passphrase is provided.
func parseKey(path, passphrasePath string) ([]byte, error) {
	keyPEM, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %s", err)
	}
	if passphrasePath != "" {
		passphrase, err := ioutil.ReadFile(passphrasePath)
		if err != nil {
			return nil, fmt.Errorf("read passphrase file: %s", err)
		}
		keyBytes, err := decryptPEMBlock(keyPEM, passphrase)
		if err != nil {
			return nil, fmt.Errorf("decrypt key: %s", err)
		}
		keyPEM, err = encodePEMKey(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("encode key: %s", err)
		}
	}
	return keyPEM, nil
}

// decryptPEMBlock decrypts the block of data.
func decryptPEMBlock(data, secret []byte) ([]byte, error) {
	block, _ := pem.Decode(data)
	if block == nil || len(block.Bytes) < 1 {
		return nil, errors.New("empty block")
	}
	decoded, err := x509.DecryptPEMBlock(block, secret)
	if err != nil {
		return nil, fmt.Errorf("decrypt block: %s", err)
	}
	return decoded, nil
}

// encodePEMKey marshals the DER-encoded private key.
func encodePEMKey(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := pem.Encode(buf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: data})
	if err != nil {
		return nil, fmt.Errorf("encode key: %s", err)
	}
	return buf.Bytes(), nil
}
